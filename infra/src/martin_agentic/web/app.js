'use strict';
let snapshot=null,connected=false,screenFilter='all',selected='SOFI';
const $=id=>document.getElementById(id);
const fmt=(v,d=2)=>v===null||v===undefined?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const money=v=>v===null||v===undefined?'—':'$'+fmt(v);
const time=v=>v?new Intl.DateTimeFormat('zh-CN',{timeZone:'America/New_York',hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'}).format(new Date(v)):'—';
const age=v=>v?(Date.now()-Date.parse(v))/1000:null;
const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n};
const pill=(text,kind='muted')=>el('span',text,'pill '+kind);
const empty=(parent,message)=>{parent.replaceChildren(el('div',message,'empty'))};
const labels={RS_TOP_DECILE:'池内 RS 未进入前10%',ADR_ABOVE_5_PERCENT:'ADR20 不超过5%',DOLLAR_VOLUME_ABOVE_50M:'20日成交额不足 $50M',EMA_STACK:'EMA9/21/50 未形成多头排列',NO_THREE_CATEGORY_SUPPORT:'未找到三类支撑共振',SCREEN_NOT_QUALIFIED:'筛选未通过',UNKNOWN:'提交结果未知，需核对',WAITING_USER:'待正式确认',READY:'待处理',RUNNING:'Agent 处理中',REPORTED:'Agent 已返回，待券商核实',OBSERVED:'券商已观察',EXPIRED:'已过期',entry:'入场',protect:'原生止损',cancel:'撤单',exit:'退出',strategy:'策略',engineering:'工程验收',manual_recovery:'人工恢复',unclassified:'未归类',ENTRY_PLANNED:'入场计划',ENTRY_AWAITING_BROKER:'等待券商订单',ENTRY_OPEN:'入场挂单中',PROTECTED:'已核实保护',NEEDS_PROTECTION:'未核实保护',EXIT_REQUESTED:'退出处理中',FLAT_VERIFIED:'已核实空仓',ACTUAL_FILL_OBSERVED:'实际成交已观察',BROKER_ORDER_OBSERVED:'券商订单已观察',AWAITING_NATIVE_CONFIRMATION:'等待正式订单确认',AUDITED_MANUAL_CLOSE:'已审计结案 · 人工接管',QUEUED_NOT_BROKER_VERIFIED:'已入队 · 尚无券商订单证据',PARAMETERS_SPECIFIED_NOT_REVIEWED:'参数已指定 · 尚未 review',AWAITING_PARAMETER_SELECTION:'拟议方案 · 等待指定参数'};
const text=v=>labels[v]||v||'—';
function header(){
 $('clock').textContent=time(new Date().toISOString())+' ET';if(!snapshot)return;
 const observation=snapshot.account_observation||{},accountAt=observation.observed_at||snapshot.runtime.observed_at,recovered=connected&&observation.source==='audited_post_close_broker_observation'&&observation.historical_only===true;
 const r=snapshot.runtime,closed=connected&&r.session_closed_safe===true,a=age(r.observed_at),fresh=connected&&r.healthy_now&&a!==null&&a<=30&&a>=0;
 $('connection').textContent=!connected?'页面连接中断':fresh?'● 数据更新中':closed?'● 会话已安全收盘':'● 观察已陈旧 / 停止';$('connection').className='pill '+(fresh||closed?'green':'amber');
 const accountAge=age(accountAt);$('age').textContent=accountAge===null?'—':Math.max(0,Math.floor(accountAge))+'s';$('pid-note').textContent=(r.process_alive?'进程存在':'进程未验证')+' · PID '+(r.pid||'—');
 document.body.classList.toggle('offline',!fresh);
 $('notice').className='notice '+(!connected?'danger':fresh||closed?'warning':'danger');
 $('notice').textContent=!connected?'页面暂时无法取得新快照。下方保留上次数据，不能视为实时交易状态。':closed?'会话结束时已核实空仓，无运行中的交易会话。以下为历史快照，报价已停止更新；历史人工恢复不代表模块实盘验收通过。':recovered?'交易会话已停止，以下为已审计结案时的历史账户快照。原保护及自动收盘验收失败仍保留，历史生命周期不代表当前持仓。':!fresh?'当前运行或数据时效检查未通过，请核对停止原因。陈旧快照不代表系统正在运行。':'当前接入真实数据，运行在订单预览模式。工程验收与策略交易分别记录；真实下单仍需正式确认，生产验收尚未完成。';
 $('updated').textContent='账户观察 '+time(accountAt)+' ET · 会话记录 '+time(r.observed_at)+' ET · 页面读取 '+time(snapshot.server_time)+' ET';
}
function render(){
 const s=snapshot,r=s.runtime,d=s.data,p=s.portfolio;
 $('account').textContent=s.account;$('equity').textContent=money(p.total_value);
 const observation=s.account_observation||{};$('account-observation-note').textContent='账户观察 '+time(observation.observed_at||r.observed_at)+' ET'+(observation.historical_only===true?' · 历史快照':'');$('candidates').textContent=d.candidate_count??'—';$('fill-count').textContent=s.synthetic_book?'—':s.fills.length;
 $('pool-note').textContent=(d.quotes??0)+' 个配置池报价 · 不含工程草案';$('fills-note').textContent=s.synthetic_book?'当前为合成账本，非真实成交':'仅已观察的券商 execution_id';
 $('mode').textContent=r.agent_mode==='review'?'真实数据 · 订单预览':r.agent_mode==='execute'?'执行模式 · 依正式确认':'只读 / 状态未知';
 $('mode-note').textContent='发布验收未通过 · 不代表无人值守实盘';$('cycle').textContent='周期 '+(r.cycle??'—')+' · ET '+time(r.observed_at);
 const pending=s.actions.filter(a=>a.effective_retired!==true&&['READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'].includes(a.state));$('pending-badge').textContent=pending.length;
 const stages=[['01','行情采集',(d.quotes??0)+' 个报价 / '+(d.bars??0)+' 根完整线',r.healthy_now],['02','特征计算',s.strategy.universe_complete?'前一完整日线 / EMA 已校验':'日线特征未就绪',!!s.strategy.universe_complete],['03','规则筛选',(d.candidate_count??0)+' 个候选 · 非预测',(d.candidate_count??0)>0],['04','操作意图',pending.length+' 个待处理动作',pending.length===0],['05','成交与保护',s.trades.length?'查看实际持仓与保护状态':'无持仓 / 尚无成交验收',false]];
 $('pipeline').replaceChildren(...stages.map(([num,title,desc,good])=>{const n=el('div',undefined,'stage '+(good?'good':'hold'));n.append(el('small',num),el('strong',title),el('span',desc));return n}));
 const symbols=s.quotes.map(q=>q.symbol);if(!symbols.includes(selected))selected=symbols[0]||'SOFI';$('symbol').replaceChildren(...symbols.map(v=>{const o=el('option',v);o.value=v;o.selected=v===selected;return o}));
 const q=s.quotes.find(q=>q.symbol===selected);$('quote-summary').replaceChildren(el('span',q?money(q.bid)+' / '+money(q.ask):'暂无有效双边报价'),el('small','Bid / Ask · 非成交回执'));
 $('quote-time').textContent=q?'报价时间 '+time(q.bid_at)+' ET':'报价未取得';chart();screen();
 const checks=[['券商实际观察',r.healthy_now?'新鲜且进程存在':'未通过'],['运行代码与当前文件',r.build_matches?'一致':'已变更 / 需重新验收'],['完成的五分钟线',(d.bars??'—')+' 根'],['待审批（券商）',s.broker_summary.pending_approvals??'—'],['本轮数据检查',(s.data_quality.errors||[]).length?'存在拒绝原因':'检查日志见时间线']];
 const indicator=s.strategy.indicator_values?.[selected];if(indicator)checks.splice(2,0,[selected+' 前日 EMA9 / 21 / 50',['9','21','50'].map(p=>indicator[p]?fmt(indicator[p].value):'—').join(' / ')]);
 $('checks').replaceChildren(...checks.map(([k,v])=>{const row=el('div',undefined,'check-row');row.append(el('span',k),el('span',String(v)));return row}));
 $('issues').textContent=[...s.issues,...s.errors,...(s.data_quality.errors||[]).map(e=>e.reason)].join(' · ')||'当前没有新增的持仓对账问题。';
 for(const kind of ['strategy','engineering']){const performance=s.performance[kind];$(kind+'-performance').replaceChildren(el('span',s.fills_truncated?'历史截断，暂不计算':performance.closed_trades?money(performance.realized_pnl):'尚无完整平仓'),el('small',performance.closed_trades+' 笔完整真实交易'));}
 plans();execution();timeline();release();header();
}
function screen(){
 const s=snapshot;const rejected=s.strategy.screening_summary?.rejected||s.data.screening?.rejected||[];
 const machines=s.strategy.machines||{};const map=new Map(rejected.map(x=>[x.symbol,x]));const symbols=[...new Set([...s.quotes.map(q=>q.symbol),...map.keys(),...Object.keys(machines)])];
 const body=$('screen-table');body.replaceChildren();
 for(const symbol of symbols){const qualified=!!machines[symbol];if(screenFilter==='qualified'&&!qualified||screenFilter==='rejected'&&qualified)continue;
  const rej=map.get(symbol),q=s.quotes.find(q=>q.symbol===symbol),row=el('tr');row.append(el('td',symbol),el('td',q?fmt(q.bid)+' / '+fmt(q.ask):'—'));
  const quoteCell=el('td');const sec=q?Math.max(age(q.bid_at),age(q.ask_at)):null;quoteCell.append(pill(sec===null?'无报价':fmt(sec,0)+'s',sec!==null&&sec<=15&&sec>=0?'green':'amber'));row.append(quoteCell);
  const metrics=rej?.metrics||machines[symbol]?.setup?.features||{};row.append(el('td',metrics.rs_percentile===undefined?'—':fmt(Number(metrics.rs_percentile)*100,1)+'%'),el('td',metrics.adr20_fraction===undefined?'—':fmt(Number(metrics.adr20_fraction)*100,2)+'%'));
  const reason=el('td');reason.append(pill(qualified?'候选 · '+text(machines[symbol].state):'未通过',qualified?'green':'muted'));
  reason.append(el('span',qualified?'满足筛选，仍需 flush/reclaim 与新鲜报价触发':(rej?.failed_checks?.map(text).join('；')||text(rej?.reason)||'尚无筛选证据'),'reason'));row.append(reason);body.append(row);
 }
 if(!body.children.length){const row=el('tr'),cell=el('td',screenFilter==='qualified'?'当前没有合格策略候选；工程验收不会出现在这里。':'暂无筛选数据','empty');cell.colSpan=6;row.append(cell);body.append(row)}
}
function chart(){
 const svg=$('chart'),rows=snapshot.bars.filter(b=>b.symbol===selected&&b.interval==='5minute').slice(-24);svg.replaceChildren();
 const ns='http://www.w3.org/2000/svg',node=(tag,attrs)=>{const n=document.createElementNS(ns,tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);return n};
 if(rows.length<2){const t=node('text',{x:25,y:100,fill:'#728087','font-size':13});t.textContent='等待至少两根完整五分钟线';svg.append(t);$('chart-range').textContent='—';return}
 const vals=rows.map(b=>Number(b.close)),lo=Math.min(...vals),hi=Math.max(...vals),span=hi-lo||.01;
 for(let i=0;i<3;i++){const y=25+i*65;svg.append(node('line',{x1:16,x2:555,y1:y,y2:y,stroke:'#e5ebe5','stroke-width':1}));const t=node('text',{x:565,y:y+4,fill:'#88948d','font-size':10});t.textContent=fmt(hi-i*span/2);svg.append(t)}
 const points=vals.map((v,i)=>[16+i/(vals.length-1)*539,25+(hi-v)/span*130]);const coords=points.map(p=>p.join(',')).join(' ');
 svg.append(node('polygon',{points:'16,165 '+coords+' 555,165',fill:'#e7f1e8'}),node('polyline',{points:coords,fill:'none',stroke:'#37755a','stroke-width':2.5,'stroke-linejoin':'round'}));
 const last=points.at(-1);svg.append(node('circle',{cx:last[0],cy:last[1],r:3.5,fill:'#37755a'}));$('chart-range').textContent=time(rows[0].begins_at)+' — '+time(rows.at(-1).available_at)+' ET · '+rows.length+' 根已完成线';
}
function plans(){const parent=$('engineering-plans');parent.replaceChildren();for(const p of snapshot.engineering_plans){const n=el('article',undefined,'draft'),title=el('div',undefined,'draft-title');title.append(el('span',p.plan.symbol+' · '+p.plan.quantity+' 股工程方案'),pill(text(p.status),'amber'));n.append(title,el('p','拟议限价 '+money(p.plan.entry_ceiling)+' · 拟议止损 '+money(p.plan.stop)+' · 计划风险（含缓冲）'+money(p.planned_risk_with_buffer)),el('small','非合格策略信号。'+(p.effective_retired===true?'历史方案已审计结案。':p.submitted?'提交状态另见券商证据。':'尚未提交；参数/订单确认状态不可由草案推断。')),el('p','草案时间 '+time(p.created_at)+' ET · 报价需在正式操作前刷新','muted'));parent.append(n)}if(!parent.children.length)empty(parent,'尚无工程验收方案；授权测试范围不等于已下单。')}
function details(value){const d=el('details'),summary=el('summary','查看脱敏参数与返回');d.append(summary,el('pre',JSON.stringify(value,null,2)));return d}
function execution(){
 const s=snapshot,parent=$('trade-cards');parent.replaceChildren();for(const t of s.trades){const n=el('div',undefined,'trade-card');n.append(pill(text(t.purpose),t.purpose==='engineering'?'amber':'green'),el('strong',t.plan.symbol+' · '+text(t.phase)),el('div',(t.effective_retired===true?'历史账本数量 ':'账本数量 ')+t.quantity+' · 记录止损 '+money(t.stop)+' · 入场均价 '+money(t.entry_price)));if(t.effective_retired===true)n.append(el('small','已审计结案；以上保留原生命周期，账户持仓以最近券商观察为准。','muted'));if(t.stop_adjustment?.status==='DEFERRED')n.append(el('small','建议收紧至 '+money(t.suggested_stop)+'，尚未执行：安全替换能力未验证。原计划止损未变。','muted'));parent.append(n)}if(!s.trades.length)empty(parent,'暂无已入队交易。草案不会建立持仓或保护状态。');
 const body=$('actions-table');body.replaceChildren();for(const a of s.actions){const p=a.payload.parameters,row=el('tr'),kind=el('td');kind.append(pill(text(a.purpose),a.purpose==='engineering'?'amber':'muted'),el('span',' '+a.payload.plan.symbol));row.append(kind,el('td',text(a.kind)),el('td',(p.quantity??'—')+' 股 · '+(p.type||'撤单')+(p.limit_price?' @ '+money(p.limit_price):'')+(p.stop_price?' 止损 '+money(p.stop_price):'')));const st=el('td');st.append(pill(a.effective_retired===true?'已结案 · 历史 '+text(a.state):text(a.state),a.effective_retired===true?'muted':['UNKNOWN','WAITING_USER'].includes(a.state)?'amber':a.state==='OBSERVED'?'green':'muted'));row.append(st,el('td',time(a.updated_at)+' ET'));const dt=el('td');dt.append(details({parameters:p,ref_id:a.payload.ref_id,report:a.report}));row.append(dt);body.append(row)}
 if(!body.children.length){const row=el('tr'),td=el('td','没有执行动作。','empty');td.colSpan=6;row.append(td);body.append(row)}
 const fb=$('fills-table');fb.replaceChildren();for(const f of s.fills){const row=el('tr');for(const v of [text(f.purpose),time(f.executed_at)+' ET',f.symbol,f.side==='buy'?'买入':'卖出',f.quantity,money(f.price),money(f.fees)])row.append(el('td',v));fb.append(row)}if(!fb.children.length){const row=el('tr'),td=el('td','暂无真实成交记录。测试与订单预览不计入。','empty');td.colSpan=7;row.append(td);fb.append(row)}
}
function timeline(){const parent=$('timeline');parent.replaceChildren();const show=$('include-observations').checked;for(const row of snapshot.audit_events){const e=row.event;if(!show&&['ACCOUNT_OBSERVED','BROKER_SOURCE_STATE'].includes(e.event_type))continue;const n=el('article',undefined,'timeline-row'),tm=el('time',time(row.recorded_at)+' ET'),body=el('div');tm.append(el('br'),el('span','落盘时间'));body.append(pill(e.synthetic?'合成演练':'真实操作 / 观察',e.synthetic?'muted':e.event_type==='ENGINEERING_TEST_PLAN'?'amber':'green'),el('strong',' '+e.event_type+(e.symbol?' · '+e.symbol:'')),el('p','发生 '+time(e.occurred_at)+' ET · '+(e.reason||e.details?.reason||e.details?.action||e.actor)));body.append(details(e));n.append(tm,body);parent.append(n)}if(!parent.children.length)empty(parent,'当前最近80条内没有非日常观察事件，可勾选显示常规账户观察。')}
function release(){
 const p=$('release'),r=snapshot.release;p.replaceChildren(pill('生产验收未通过','amber'));
 const gates=[['当前版本软件回归',r.software_test_evidence_current],['实时运行健康',snapshot.runtime.healthy_now],['模块成交→保护→退出',r.module_functional_closed_loop_validated],['保护覆盖在30秒内',r.protection_timeliness_validated],['新模块实盘验收',r.supervised_live_smoke_passed]];
 for(const [label,value] of gates){const row=el('div',undefined,'check-row');row.append(el('span',label),pill(value===true?'通过':value===false?'未通过':'无证据',value===true?'green':'amber'));p.append(row)}
 for(const c of snapshot.recovery_cases||[]){const row=el('p','恢复案 '+c.id+' · '+c.status+' · '+(c.manual_takeover?'历史人工接管':'')+'；恢复不清除原失败记录。','muted');p.append(row)}
 for(const b of r.blockers||[])p.append(el('p',b,'muted'));
 p.append(el('p','验收报告 '+time(r.as_of)+' ET；实机睡眠/认证恢复及策略盈利尚未验证。报告不批准交易。','muted'));
}
async function refresh(){try{const response=await fetch('/api/state',{cache:'no-store',signal:AbortSignal.timeout(5000)});if(!response.ok)throw new Error('HTTP '+response.status);snapshot=await response.json();connected=true;render()}catch(e){connected=false;header();if(!snapshot){$('notice').textContent='本机快照暂时不可用，请检查 dashboard 服务。';$('notice').className='notice danger'}}}
for(const b of document.querySelectorAll('[data-tab]'))b.addEventListener('click',()=>{for(const n of document.querySelectorAll('[data-tab]'))n.classList.toggle('active',n===b);for(const n of document.querySelectorAll('.tab-panel'))n.classList.toggle('hidden',n.id!==b.dataset.tab)});
for(const b of document.querySelectorAll('[data-screen]'))b.addEventListener('click',()=>{screenFilter=b.dataset.screen;for(const n of document.querySelectorAll('[data-screen]'))n.classList.toggle('active',n===b);if(snapshot)screen()});
$('symbol').addEventListener('change',()=>{selected=$('symbol').value;if(snapshot)render()});$('include-observations').addEventListener('change',()=>{if(snapshot)timeline()});
refresh();setInterval(refresh,2000);setInterval(()=>{header();if(snapshot)screen()},1000);
