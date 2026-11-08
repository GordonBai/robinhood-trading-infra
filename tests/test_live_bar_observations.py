from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_signals import LiveSignals, bar_observation_identity, filter_unchanged_bars


class LiveBarObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['session_date']='2026-10-15';policy['capture_only']=True
        self.now=datetime.fromisoformat('2026-10-15T14:00:00+00:00')
        self.book=ExecutionBook(Path(self.temp.name)/'book.sqlite',policy,simulation=True,clock=lambda:self.now)
        self.addCleanup(self.book.close)
        self.signals=LiveSignals(self.book)
        self.bar={'symbol':'SOFI','begins_at':'2026-10-15T13:30:00+00:00',
                  'open':'14','high':'14.1','low':'13.9','close':'14.05','volume':100,
                  'interval':'5minute','bounds':'regular','adjustment':'split',
                  'observed_at':self.now.isoformat(),'source':'get_equity_historicals','record_index':0}

    def event(self,bar,event_id='bar-1'):
        return {'id':event_id,'kind':'bar','at':self.now.isoformat(),
                'source_sha256':'fixture','synthetic_input':True,'data':bar}

    def test_only_committed_bars_can_be_omitted(self):
        self.assertEqual(filter_unchanged_bars([self.bar],self.signals.state()),([self.bar],0))
        self.signals.consume(self.event(self.bar))
        self.assertEqual(filter_unchanged_bars([self.bar],self.signals.state()),([],1))
        # A separate SQLite reader sees the committed cache, not a local hint.
        import sqlite3
        with sqlite3.connect(Path(self.temp.name)/'book.sqlite') as reader:
            state=json.loads(reader.execute('SELECT payload FROM live_strategy WHERE id=1').fetchone()[0])
        self.assertEqual(filter_unchanged_bars([self.bar],state),([],1))

    def test_metadata_and_numeric_format_do_not_create_bar_revisions(self):
        self.signals.consume(self.event(self.bar))
        later=dict(self.bar,observed_at='2026-10-15T14:00:10+00:00',record_index=12,open='14.000000')
        self.assertEqual(filter_unchanged_bars([later],self.signals.state()),([],1))

    def test_changed_unqualified_bar_is_emitted_and_archived_under_existing_policy(self):
        self.signals.consume(self.event(self.bar))
        revised=dict(self.bar,volume=101)
        self.assertEqual(filter_unchanged_bars([revised],self.signals.state()),([revised],0))
        self.signals.consume(self.event(revised,'revision'))
        saved=json.loads(self.book.db.execute('SELECT payload FROM live_event_archive WHERE id=?',('revision',)).fetchone()[0])
        self.assertEqual(saved['data']['volume'],101)
        self.assertEqual(filter_unchanged_bars([revised],self.signals.state()),([],1))

    def test_archive_failure_cannot_prime_duplicate_filter(self):
        with patch('martin_luk.live_signals.archive_decision',side_effect=OSError('disk fixture')):
            with self.assertRaises(OSError):self.signals.consume(self.event(self.bar))
        self.assertEqual(filter_unchanged_bars([self.bar],self.signals.state()),([self.bar],0))

    def test_new_bar_and_changed_adjustment_are_not_omitted(self):
        self.signals.consume(self.event(self.bar))
        next_bar=dict(self.bar,begins_at='2026-10-15T13:35:00+00:00')
        adjustment=dict(self.bar,adjustment='raw')
        emitted,count=filter_unchanged_bars([self.bar,next_bar,adjustment],self.signals.state())
        self.assertEqual(emitted,[next_bar,adjustment]);self.assertEqual(count,1)

    def test_filter_does_not_mutate_owner_snapshot(self):
        self.signals.consume(self.event(self.bar))
        state=self.signals.state();before=deepcopy(state)
        filter_unchanged_bars([self.bar,dict(self.bar,close='14.06')],state)
        self.assertEqual(state,before)

    def test_collection_preserves_quotes_original_indices_and_raw_batch(self):
        from types import SimpleNamespace
        from martin_luk.live_session import collect_live_events
        self.signals.consume(self.event(self.bar))
        state=self.signals.state()
        state.update(universe_date='2026-10-15',screening_summary={},indicator_values={})
        next_bar=dict(self.bar,begins_at='2026-10-15T13:35:00+00:00')
        quote={'symbol':'SOFI','bid':'14','ask':'14.01','observed_at':self.now.isoformat()}
        normalized={'data_usable':True,'bars':[self.bar,next_bar],'quotes':[quote]}
        symbols=['AAPL','AMD','HOOD','IONQ','META','NVDA','PLTR','RKLB','SOFI','TSLA']
        with patch('martin_agentic.live_session.CodexMarketTransport'), \
             patch('martin_agentic.live_session.RobinhoodReader') as reader, \
             patch('martin_agentic.live_session.inspect_recording',return_value=normalized), \
             patch('martin_agentic.live_session.persist_raw_record') as raw:
            reader.return_value.fetch.return_value={'fixture':'captured raw response'}
            events,summary=collect_live_events(SimpleNamespace(state=lambda:state),
                                              symbols=symbols,clock=lambda:self.now)
        self.assertEqual([event['kind'] for event in events],['bar','quote'])
        self.assertTrue(events[0]['id'].endswith('-bar1'))
        self.assertEqual(events[0]['data'],next_bar)
        self.assertEqual(events[1]['data'],quote)
        self.assertEqual(summary['unchanged_bar_observations_omitted'],1)
        self.assertEqual(summary['new_or_changed_bars'],1)
        self.assertEqual(raw.call_count,1)
        self.assertEqual(json.loads(raw.call_args.args[1])['records'],
                         [{'fixture':'captured raw response'}]*2)


if __name__=='__main__':unittest.main()
