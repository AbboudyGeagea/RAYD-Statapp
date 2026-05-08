-- 0032: AI documentation chunks
-- Stores guidance text for the AI assistant so navigation help
-- can be updated via admin UI without a code deploy.

CREATE TABLE IF NOT EXISTS ai_doc_chunks (
    id          SERIAL PRIMARY KEY,
    section     TEXT    NOT NULL,           -- display name, e.g. "User Management"
    keywords    TEXT    NOT NULL,           -- comma-separated trigger words
    content     TEXT    NOT NULL,           -- plain-English guidance injected into AI context
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order  INT     NOT NULL DEFAULT 0,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_doc_chunks_active ON ai_doc_chunks(is_active);

-- Seed: mirrors the hardcoded APP NAVIGATION that was previously in the system prompt.
-- Edit or extend these rows from Admin > AI Docs after deployment.
INSERT INTO ai_doc_chunks (section, keywords, content, sort_order) VALUES
('Viewer Dashboard',
 'dashboard,home,login,briefing,kpi,viewer,start',
 'The Viewer Dashboard is the home page after login. It shows a daily briefing, today''s study counts, quick KPIs, and proactive alerts.',
 10),

('Radiology Stats',
 'reports,report 22,radiology stats,studies,statistics,stat,fact',
 'Radiology Stats (Report 22) is under Reports in the sidebar. It shows study counts filtered by date range, modality, and physician, with charts and CSV export.',
 20),

('Modality and TAT',
 'modality,tat,turnaround,report 25,timing,wait time',
 'Modality & TAT (Report 25) is under Reports. It shows study counts and turnaround times broken down by imaging modality.',
 30),

('Shift Analysis',
 'shift,shift analysis,report 23,morning,afternoon,night,shift report',
 'Shift Analysis (Report 23) is under Reports. It shows study volumes split by morning, afternoon, and night shifts.',
 40),

('Order Audit',
 'order,orders,order audit,report 27,orphan,pending,order status',
 'Order Audit (Report 27) is under Reports. It tracks imaging orders, fulfilment status, and orphaned orders that have no linked study.',
 50),

('CD DVD Report',
 'cd report,dvd report,report 30,media report,disc report',
 'The CD/DVD Report (Report 30) is under Reports. It summarises discs burned per period and modality.',
 60),

('Storage Audit',
 'storage,disk,space,gb,storage audit,report 29',
 'Storage Audit (Report 29) is under Reports. It shows daily DICOM storage ingestion in GB and flags week-over-week growth anomalies.',
 70),

('Super Report',
 'super report,aggregated,summary,all reports,combined report',
 'The Super Report is under Reports. It combines all major report sections into one printable aggregated view.',
 80),

('ORU Analytics',
 'oru,hl7,word cloud,critical,radiology report,nlp,oru analytics',
 'ORU Analytics processes incoming HL7 radiology reports. It shows a word cloud of common findings, a critical findings log, and NLP statistics.',
 90),

('Custom Reports',
 'custom report,ad hoc,custom,build report,custom query',
 'Custom Reports lets you build ad-hoc queries on study data by selecting fields, filters, and date ranges without writing SQL.',
 100),

('Patient CD Log',
 'patient cd,cd log,cd print,dvd log,burned,disc log,cd burned,patient disc',
 'The Patient CD Log tracks every CD/DVD burned for a patient. Search by patient name, date range, or modality. Access it from the sidebar under RIS.',
 110),

('ER Dashboard',
 'er,emergency,unread,er dashboard,sla,urgent,er studies',
 'The ER Dashboard shows unread studies from the emergency department and tracks SLA compliance in real time.',
 120),

('Capacity Ladder',
 'capacity,utilization,ladder,device schedule,schedule,ae capacity,capacity ladder',
 'The Capacity Ladder shows each imaging device''s utilization vs its scheduled opening minutes for the day or week.',
 130),

('AI Assistant',
 'ai assistant,chat,ask,natural language,ai chat,assistant',
 'The AI Assistant (this interface) lets you ask data questions in natural language and receive answers drawn from live statistics.',
 140),

('AI Report Intelligence',
 'ai report,intelligence,anomaly,forecast,trend report,ai intelligence',
 'AI Report Intelligence generates an automated anomaly report with trend analysis across all key metrics.',
 150),

('Revenue Intelligence',
 'revenue,financial,billing,income,enterprise,revenue intelligence',
 'Revenue Intelligence (Enterprise tier) provides billing and financial analytics linked to study volumes.',
 160),

('Patient Portal',
 'patient portal,self service,patient access,results portal,portal',
 'The Patient Portal (Enterprise tier) lets patients access their own radiology results online with a secure login.',
 170),

('Live AE Status',
 'live,ae status,real time,dicom feed,device live,live status',
 'Live AE Status (Enterprise tier) shows a real-time feed of DICOM device activity and connectivity.',
 180),

('User Management',
 'user,users,add user,edit user,role,admin user,deactivate,password,user management,new user',
 'Go to Admin > User Management in the sidebar to add, edit, or deactivate users and assign roles (admin, viewer, viewer2).',
 190),

('DB Manager',
 'db manager,database,connection,oracle,pacs connection,db params,database manager',
 'Admin > DB Manager is where you configure external database connections such as the Oracle PACS source.',
 200),

('Modality Map',
 'modality map,ae title,aetitle,mapping,device name,ae map,modality mapping',
 'Admin > Modality Map lets you map AE titles to canonical modality names and set per-device daily capacity.',
 210),

('Documentation',
 'documentation,docs,guide,help,manual,tutorial,how to,how do,how can,where is,feature guide,help guide',
 'The full user guide is under Help > Documentation in the sidebar. It covers every feature with step-by-step instructions.',
 220),

('Scope — What RAYD Does Not Do',
 'schedule,scheduling,book,booking,new study,create study,add study,register,appointment,worklist,order new,create order,refer,referral form',
 'RAYD is a radiology analytics and statistics platform — it reads and displays data but does not create studies, schedule appointments, or manage worklists. To schedule a new study or create an order, use your RIS or PACS system directly.',
 999);
