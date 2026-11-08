"""Durable per-session model call reservations, independent of action outcomes."""
import sqlite3
from datetime import datetime,timezone


class WorkerBudget:
    def __init__(self,path,session_date,*,entry_limit=20,total_limit=40,clock=None):
        if type(entry_limit) is not int or type(total_limit) is not int or not 0<=entry_limit<=total_limit:
            raise ValueError('INVALID_WORKER_BUDGET')
        self.db=sqlite3.connect(path,timeout=5);self.date=session_date;self.clock=clock or (lambda:datetime.now(timezone.utc))
        self.db.executescript('''CREATE TABLE IF NOT EXISTS worker_call_reservations(action_id TEXT PRIMARY KEY,session_date TEXT NOT NULL,kind TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS worker_budget_limits(session_date TEXT PRIMARY KEY,entry_limit INTEGER NOT NULL,total_limit INTEGER NOT NULL);''')
        if 'reserved_at' not in {r[1] for r in self.db.execute('PRAGMA table_info(worker_call_reservations)')}:
            self.db.execute("ALTER TABLE worker_call_reservations ADD COLUMN reserved_at TEXT");self.db.commit()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            prior=self.db.execute('SELECT entry_limit,total_limit FROM worker_budget_limits WHERE session_date=?',(session_date,)).fetchone()
            if prior and (entry_limit>prior[0] or total_limit>prior[1]):
                raise ValueError('WORKER_BUDGET_CANNOT_INCREASE_ON_RESTART')
            self.db.execute('INSERT INTO worker_budget_limits VALUES(?,?,?) ON CONFLICT(session_date) DO UPDATE SET entry_limit=excluded.entry_limit,total_limit=excluded.total_limit',(session_date,entry_limit,total_limit))
            self.db.commit()
        except BaseException:
            self.db.rollback();self.db.close();raise
        self.entry_limit=entry_limit;self.total_limit=total_limit

    def status(self):
        limits=self.db.execute('SELECT entry_limit,total_limit FROM worker_budget_limits WHERE session_date=?',(self.date,)).fetchone()
        self.entry_limit,self.total_limit=limits
        rows=self.db.execute('SELECT kind,reserved_at FROM worker_call_reservations WHERE session_date=?',(self.date,)).fetchall()
        total=len(rows);entries=sum(r[0]=='entry' for r in rows)
        return {'total_calls_reserved':total,'entry_calls_reserved':entries,'total_limit':self.total_limit,
            'entry_limit':self.entry_limit,'entries_available':total<self.total_limit and entries<self.entry_limit,
            'management_available':total<self.total_limit,'reservation_time_basis':'local_utc_reservation_not_token_usage',
            'last_reserved_at':max((r[1] for r in rows if r[1]),default=None),'legacy_reservations_without_timestamp':sum(r[1] is None for r in rows)}

    def has_reservation(self,action_id):
        """Whether this exact action already holds a durable reservation."""
        return self.db.execute('SELECT 1 FROM worker_call_reservations WHERE action_id=?',
                               (action_id,)).fetchone() is not None

    def reserve(self,action):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            if self.db.execute('SELECT 1 FROM worker_call_reservations WHERE action_id=?',(action['action_id'],)).fetchone():
                self.db.rollback();return False
            state=self.status()
            if not state['management_available'] or (action['kind']=='entry' and not state['entries_available']):
                self.db.rollback();return False
            self.db.execute('INSERT INTO worker_call_reservations(action_id,session_date,kind,reserved_at) VALUES(?,?,?,?)',(action['action_id'],self.date,action['kind'],self.clock().astimezone(timezone.utc).isoformat()))
            self.db.commit();return True
        except BaseException:self.db.rollback();raise

    def close(self):self.db.close()
