"""Build the Phase 3 architecture PDF. Missing real evidence keeps it a DRAFT.

python scripts/build_report.py [--final] [--evidence path/to/evidence.json]
Requires reportlab; source text/diagrams are maintained in this file.
"""

import argparse
import json
import math
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
                                PageBreak, Flowable, Image)

ROOT = Path(__file__).resolve().parents[1]
NAVY = colors.HexColor("#142D47")
TEAL = colors.HexColor("#087F8C")
MUTED = colors.HexColor("#526577")
PALE = colors.HexColor("#EDF5F7")
AMBER = colors.HexColor("#FFF1D4")
WIDTH = A4[0] - 96


def resolved(path):
    return ROOT / path if path else None


def numeric(value, positive=False):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and (value > 0 if positive else value >= 0)


def missing_evidence(e):
    missing = []
    if not e.get("workspace_run_id"):
        missing.append("successful Gold workspace run id")
    if e.get("gold_test_run_passed") is not True:
        missing.append("passing notebook 82 confirmation")
    counts = e.get("gold_row_counts", {})
    if not all(numeric(counts.get(k), positive=True) for k in ("daily_rows", "monthly_rows", "aggregated_rows")):
        missing.append("positive Gold row counts")
    elif counts['daily_rows'] != counts['aggregated_rows']:
        missing.append("reconciled daily/monthly observation counts")
    run = e.get("typical_incremental_run", {})
    if not run.get("run_id") or not numeric(run.get("wall_seconds"), positive=True) or not numeric(run.get("summed_step_seconds")):
        missing.append("measured normal incremental-run timings")
    if not numeric(run.get("measured_cost_usd")):
        missing.append("actual incremental-run cost (explicit zero for confirmed Free Edition)")
    dag = e.get("dag", {})
    if dag.get("kind") not in ("query_profile", "spark_ui"):
        missing.append("DAG evidence kind")
    if dag.get("kind") == "query_profile" and dag.get("instructor_accepted_query_profile") is not True:
        missing.append("instructor acceptance of Query Profile instead of Spark UI")
    if not dag.get("analysis"):
        missing.append("analysis of the actual executed DAG")
    for name, path in [("DAG screenshot", dag.get("image")),
                       ("Power BI file", e.get("power_bi", {}).get("pbix_path")),
                       ("dashboard screenshot", e.get("power_bi", {}).get("dashboard_image")),
                       ("Power BI model screenshot", e.get("power_bi", {}).get("model_image"))]:
        if not path or not resolved(path).is_file() or resolved(path).stat().st_size == 0:
            missing.append(name)
    fin = e.get("finops", {})
    for key in ("enterprise_region", "pricing_source", "assumptions"):
        if not fin.get(key):
            missing.append(f"FinOps {key}")
    for key in ("enterprise_cost_per_run_usd", "storage_monthly_usd", "bi_monthly_usd", "other_monthly_usd"):
        if not numeric(fin.get(key), positive=key == "enterprise_cost_per_run_usd"):
            missing.append(f"FinOps {key}")
    return missing


class Diagram(Flowable):
    def __init__(self, kind):
        super().__init__()
        self.kind = kind
        self.width = WIDTH
        self.height = 235 if kind == "architecture" else 205

    def draw(self):
        c = self.canv
        def box(x, y, w, h, title, lines, fill=PALE):
            c.setFillColor(fill); c.setStrokeColor(TEAL)
            c.roundRect(x, y, w, h, 7, fill=1, stroke=1)
            c.setFillColor(NAVY); c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(x+w/2, y+h-18, title)
            c.setFont("Helvetica", 8)
            for i, line in enumerate(lines):
                c.drawCentredString(x+w/2, y+h-33-i*12, line)
        def arrow(x1,y1,x2,y2, label=None):
            c.setStrokeColor(TEAL); c.setFillColor(TEAL); c.setLineWidth(1.5)
            c.line(x1,y1,x2,y2)
            angle = math.atan2(y2-y1,x2-x1)
            p = c.beginPath(); p.moveTo(x2,y2)
            p.lineTo(x2-6*math.cos(angle-0.45), y2-6*math.sin(angle-0.45))
            p.lineTo(x2-6*math.cos(angle+0.45), y2-6*math.sin(angle+0.45)); p.close()
            c.drawPath(p,fill=1,stroke=0)
            if label:
                c.setFillColor(MUTED); c.setFont("Helvetica",8)
                c.drawCentredString((x1+x2)/2, (y1+y2)/2+5, label)
        if self.kind == "architecture":
            w = 145
            box(0,163,w,66,"Public sources",["Yahoo Finance / yfinance", "Wikipedia constituents"])
            box(176,163,w,66,"Laptop extraction",["Full / incremental JSON", "Manifest uploaded last"])
            arrow(145,196,176,196)
            box(352,163,w,66,"Raw Volume",["Unity Catalog landing", "Original source payloads"])
            arrow(321,196,352,196)
            box(352,68,w,66,"Bronze / Silver",["Explicit schema, try_cast", "MERGE, quarantine, revisions"])
            arrow(425,163,425,134)
            box(176,68,w,66,"Gold star schema",["Date / Ticker dimensions", "Daily + monthly facts"])
            arrow(352,101,321,101)
            box(0,68,w,66,"Power BI Desktop",["Direct warehouse connection", "Import model + visuals"])
            arrow(176,101,145,101)
            c.setFillColor(MUTED); c.setFont("Helvetica",8)
            c.drawString(0,40,"All Spark layers share execution logging; Gold validation gates BI refresh.")
            c.drawString(0,25,"Deployment: Databricks Free Edition (AWS), serverless compute + SQL warehouse.")
        else:
            box(15,126,190,70,"dim_ticker",["PK ticker_key (stable BIGINT)","Unique ticker; latest descriptions"])
            box(292,126,190,70,"dim_date",["PK date_key (YYYYMMDD)","Unique contiguous calendar_date"])
            box(15,10,190,72,"fact_daily_market",["PK (ticker_key, date_key)","Prices, returns, MA7/30, risk"])
            box(292,10,190,72,"fact_monthly_market",["PK (ticker_key, date_key)","Monthly activity + correlations"])
            arrow(110,126,110,82,"1 : many")
            arrow(387,126,387,82,"1 : many")
            arrow(205,145,322,82)
            arrow(292,145,175,82)


def build(e, final=False):
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="MPTitle", fontName="Helvetica-Bold", fontSize=32,
                              leading=37, textColor=NAVY, spaceAfter=14))
    styles.add(ParagraphStyle(name="MPHeading", fontName="Helvetica-Bold", fontSize=18,
                              leading=23, textColor=NAVY, spaceAfter=14))
    styles.add(ParagraphStyle(name="MPBody", fontName="Helvetica", fontSize=10,
                              leading=15, textColor=NAVY, spaceAfter=10))
    styles.add(ParagraphStyle(name="MPSub", fontName="Helvetica-Bold", fontSize=11,
                              leading=15, textColor=TEAL, spaceBefore=10, spaceAfter=7))
    styles.add(ParagraphStyle(name="MPCell", fontSize=8.5, leading=12, textColor=NAVY))
    styles.add(ParagraphStyle(name="MPSmall", fontSize=8, leading=11, textColor=MUTED, spaceAfter=7))
    story = []
    def p(text, style="MPBody"):
        return Paragraph(text, styles[style])
    def add(text, style="MPBody"):
        story.append(p(text,style))
    def page(number, title):
        if story:
            story.append(PageBreak())
        add(f"{number:02d} / {title}","MPHeading")
    def sub(text):
        add(text,"MPSub")
    def table(headers, rows, widths=None):
        content = [[p(escape(str(v)),"MPCell") for v in headers]]
        content += [[p(escape(str(v)),"MPCell") for v in row] for row in rows]
        t=Table(content,colWidths=widths or [WIDTH/len(headers)]*len(headers),repeatRows=1,hAlign="LEFT")
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),PALE), ("LINEBELOW",(0,0),(-1,0),1,TEAL),
            ("VALIGN",(0,0),(-1,-1),"TOP"), ("LEFTPADDING",(0,0),(-1,-1),8),
            ("RIGHTPADDING",(0,0),(-1,-1),8), ("TOPPADDING",(0,0),(-1,-1),7),
            ("BOTTOMPADDING",(0,0),(-1,-1),7),
            ("LINEBELOW",(0,1),(-1,-1),0.35,colors.HexColor("#D9E2E9"))]))
        story.append(t); story.append(Spacer(1,12))
    def figure(path, missing, maxheight=270):
        file = resolved(path)
        if file and file.is_file():
            w,h=ImageReader(str(file)).getSize()
            scale=min(WIDTH/w,maxheight/h)
            story.append(Image(str(file),width=w*scale,height=h*scale,hAlign="CENTER"))
            story.append(Spacer(1,10))
        else:
            t=Table([[p(escape(missing),"MPBody")]],colWidths=[WIDTH],rowHeights=[95])
            t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),AMBER),
                                   ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
                                   ("LEFTPADDING",(0,0),(-1,-1),15),
                                   ("RIGHTPADDING",(0,0),(-1,-1),15)]))
            story.append(t); story.append(Spacer(1,12))
    def shown(value, unit=""):
        if value is None:
            return "Awaiting measurement"
        return f"{value:,.3f}{unit}" if isinstance(value,float) else f"{value}{unit}"

    add("MARKETPULSE","MPTitle")
    add("Lakehouse Architecture &amp; Business Intelligence","MPHeading")
    add("DS-3001 / Data Analysis and Visualization / Phase 3","MPSub")
    status="FINAL EVIDENCE PACKAGE" if final else "DRAFT - FINAL EVIDENCE PENDING"
    table(["Document status"],[[status]])
    add("<b>Engineering and report:</b> Soban (24L-2545)<br/>"
        "<b>Power BI:</b> Saad Jahangir (24L-2516)<br/>"
        "<b>Earlier phases:</b> completed jointly by Soban and Saad.")
    sub("Purpose")
    add("MarketPulse converts daily market prices into a reusable analytical model. "
        "The pipeline retains raw inputs, validates and reconciles typed records, and computes "
        "business measures before Power BI reads them. Its three questions concern closing-price "
        "trends, relative volatility, and trading volume versus price movement.")
    sub("Scope and evidence")
    add("The Phase 1 proposal used eight tickers and five years. Phase 2 expanded extraction "
        "to the current S&amp;P constituent universe and a ten-year baseline. The saved Phase 2 "
        "idempotency export documents 1,225,467 Silver price rows. That historical evidence is "
        "separate from validation of the new Gold implementation.")
    table(["Item","Status"],[
        ["Phase 3 workspace run", e.get("workspace_run_id") or "Not supplied"],
        ["Gold integration notebook 82", "Confirmed passed" if e.get("gold_test_run_passed") else "Awaiting workspace execution"],
        ["Power BI deliverable", "Provided" if resolved(e.get('power_bi',{}).get('pbix_path')) and resolved(e['power_bi']['pbix_path']).is_file() else "Owned by Saad; pending"],
    ],[180,WIDTH-180])
    add('Repository: <link href="https://github.com/Soban06/MarketPulse" color="#087F8C">github.com/Soban06/MarketPulse</link>',"MPSmall")

    page(2,"End-to-end architecture")
    story.append(Diagram("architecture"))
    sub("Execution boundary")
    add("Source extraction runs on the laptop because the implemented Free Edition workspace "
        "restricted outbound access to the source hosts. The uploader writes manifest.json last. "
        "A batch without a manifest is incomplete and is ignored. Raw payloads live in a Unity "
        "Catalog Volume; Bronze/Silver/Gold tables are managed Delta tables.")
    sub("Pipeline integration")
    add("The checked-in Databricks bundle runs setup and then notebook 04. The Python pipeline "
        "entry point calls Bronze, Silver prices, Silver profiles and Gold in sequence under a "
        "shared pipeline_run_id. It does not continue into Gold after upstream failure. The local "
        "runner combines extraction, upload and job invocation into one daily command.")
    add("The job permits one concurrent run. Gold always runs after Silver, including a retry "
        "where Silver has no new pending batches. Automatic Power BI Service refresh is not "
        "assumed: the Desktop file is refreshed only after the entire job succeeds.")

    page(3,"Gold dimensional model")
    story.append(Diagram("star"))
    table(["Object","Business grain / role"],[
        ["dim_ticker", "One company symbol; stable numeric key, latest Type 1 descriptive attributes."],
        ["dim_date", "One contiguous calendar date, including weekends; no fabricated daily price rows."],
        ["fact_daily_market", "One ticker per observed trading date; prices and derived business metrics."],
        ["fact_monthly_market", "One ticker per observed month; aggregate counts, volume and correlations."],
    ],[150,WIDTH-150])
    add("Facts reference both dimensions. The monthly date key is the first calendar date of "
        "the month. Daily and monthly facts are never joined directly in Power BI. Primary and "
        "foreign keys are informational in Databricks; explicit key, cardinality and orphan "
        "checks establish integrity. NOT NULL constraints are enforced.")
    add("Ticker descriptions use Type 1 updates. Existing keys are retained when new symbols "
        "arrive or profiles are absent. Historical sector views therefore use current known "
        "classification. A complete column dictionary and ER diagram are versioned with code.")

    page(4,"Metric definitions and analytical meaning")
    table(["Metric","Definition / interpretation"],[
        ["Daily return", "Current adjusted close / previous observation adjusted close - 1. First or invalid adjacent prices produce null."],
        ["MA 7 / MA 30", "Arithmetic mean of close over 7 / 30 trading observations, current included. Full valid windows required."],
        ["Rolling volatility", "Sample standard deviation over 30 valid daily returns. Earliest value is observation 31."],
        ["Annualized volatility", "Rolling volatility x sqrt(252); a convention, not a risk forecast."],
        ["Relative volume", "Current volume / mean of preceding 30 observations. Excludes current row; zero/incomplete baseline produces null."],
        ["Monthly correlation", "Pairwise Pearson correlation of raw/relative volume with absolute daily return within a ticker-month; undefined variance gives null."],
    ],[135,WIDTH-135])
    add("Windows are computed on complete Silver history before Power BI filters. Trading "
        "observations bridge weekends and holidays without inserting false zero returns. Prices, "
        "moving averages, volatility and correlations are non-additive. Volume and observation "
        "counts can be summed. Percentages remain decimal fractions and receive percentage "
        "formatting only in Power BI.")
    sub("Limits on interpretation")
    add("Volume association does not establish causation. The current constituent universe "
        "creates survivorship bias in historical comparisons. Current company-profile metrics "
        "are snapshots, so this model does not claim historical valuations or historical index "
        "membership. The monthly summary does not assert every month has complete market coverage.")

    page(5,"Engineering decisions and governance")
    table(["Decision","Reason and consequence"],[
        ["Explicit schemas", "Bronze source values are read as strings. Unknown price/constituent fields are rescued; Silver try_cast and quarantine handle invalid types."],
        ["Conditional MERGE", "Natural keys reconcile Silver. Gold recalculates complete history but writes only changed values, preserving timestamps on identical reruns."],
        ["Pinned source versions", "Each Gold run reads fixed Silver Delta versions and records them in audit parameters. Run one writer at a time."],
        ["Full-history windows", "Backfilled observations and price revisions can affect subsequent returns/windows. Full recomputation favors correctness at the present scale."],
        ["No Hive partitions", "This dataset is far below 1 TB. Many small ticker/day partitions add overhead. Optional clustering targets trade_date and ticker_key."],
        ["Operational views", "BI reads execution steps and aggregated quarantine/revisions through Gold views. Reject totals are taken from quarantine, not summed repeatedly across step logs."],
    ],[135,WIDTH-135])
    sub("PII and access")
    add("OHLCV records are public company-market observations. However, extraction now stores "
        "complete Yahoo profile payloads, which can include public officer names and other "
        "personal attributes. Raw must not be described categorically as PII-free without payload "
        "review. Explicit downstream schemas select only approved company fields; Gold excludes "
        "officer details. Audit identities stay in ops and are omitted from the BI execution view. "
        "Review error messages before publication. Credentials and landing batches are git-ignored; "
        "screenshots must redact secrets and unnecessary identities.")

    page(6,"Validation, logging and recovery")
    add("The local analytical tests execute the production SELECT statements in DuckDB with "
        "synthetic data and independently calculated expectations. They cover moving averages, "
        "adjusted returns, volatility, volume baselines, correlations, invalid values and "
        "correction/backfill propagation. This is an independent SQL calculation check, not a "
        "substitute for Spark/Delta integration testing.")
    if e.get('local_test_count'):
        add(f"Local verification: <b>{int(e['local_test_count'])} tests passed</b>. "
            "The successful Databricks integration export is saved separately with its real outputs.")
    add("Notebook 82 tests real Delta operations in uniquely named scratch schemas: audit-schema "
        "migration, allocated keys, missing profiles, identical reruns, historical corrections, "
        "source deletions, failed preflight and retry. Existing Phase 2 tests are extended so "
        "their cleanup cannot target production Gold.")
    counts=e.get("gold_row_counts",{})
    table(["Gold reconciliation","Measured value"],[
        ["Daily fact rows", shown(counts.get('daily_rows'))],
        ["Monthly groups", shown(counts.get('monthly_rows'))],
        ["Daily observations represented in monthly groups", shown(counts.get('aggregated_rows'))],
        ["Workspace integration confirmation", "Passed" if e.get('gold_test_run_passed') else "Pending"],
    ],[290,WIDTH-290])
    sub("Failure behavior")
    add("Gold stages each result once, checks keys, then uses Delta MERGE. Empty Silver prices "
        "fail preflight and preserve existing Gold facts. Actual row inserts, updates and deletes "
        "come from Delta metrics; rows_aggregated counts monthly output groups. Timings include "
        "calculation, validation and writes. A failed final audit write fails the pipeline so "
        "success is not silently reported without its log.")
    add("Delta commits are atomic per table, not across the whole star schema. A failure can "
        "leave partially refreshed Gold tables. A complete successful retry is required before "
        "Power BI Import refresh. Staging cleanup errors are surfaced for follow-up.")

    page(7,"Executed DAG and performance evidence")
    dag=e.get("dag",{})
    figure(dag.get("image"),"DAG screenshot pending: run the monthly Gold aggregation in notebook 93 and capture its real Query Profile (or accepted Spark UI evidence).",300)
    if dag.get("analysis"):
        add(escape(dag['analysis']))
    else:
        add("Execution analysis is pending. The monthly aggregation is expected to scan Gold "
            "daily columns and aggregate by ticker/month. An exchange may redistribute groups. "
            "These expectations must be replaced with the operators, timings and row/byte "
            "statistics from the executed graph. Do not claim predicate pushdown, broadcast joins "
            "or eliminated shuffles without observing them.")
    sub("Free Edition constraint")
    add("Databricks serverless does not expose Spark UI. Query Profile is its supported "
        "diagnostic surface. Submitting that instead of a literal Spark UI screenshot requires "
        "instructor acceptance; otherwise the workload must run on suitable classic compute.")
    sub("Optimization experiment")
    add("Notebook 93 records three analytical-query elapsed times before optional Liquid "
        "Clustering/OPTIMIZE and three afterward, alongside table details. Repeated runs may "
        "benefit from caches. Record actual scans and operators; this small dataset may show "
        "little speed improvement. Avoid unnecessary optimization on every daily run.")
    if e.get('benchmark_before_seconds'):
        add("Observed read-query elapsed times: " + escape(str(e['benchmark_before_seconds'])) +
            " seconds. These repeated measurements may include cache effects; no post-OPTIMIZE "
            "speedup is claimed.","MPSmall")

    page(8,"FinOps evaluation")
    run=e.get("typical_incremental_run",{}); fin=e.get("finops",{})
    table(["Observed incremental run","Evidence"],[
        ["Run id", run.get('run_id') or "Not supplied"],
        ["Spark pipeline elapsed time", shown(run.get('wall_seconds'),' seconds')],
        ["Summed logged step time", shown(run.get('summed_step_seconds'),' seconds')],
        ["Billed DBUs", shown(run.get('billed_dbus'))],
        ["Actual run cost", shown(run.get('measured_cost_usd'),' USD')],
    ],[220,WIDTH-220])
    if run.get('scope'):
        add(escape(run['scope']),"MPSmall")
    steady=e.get('steady_state_refresh',{})
    if steady:
        add("Full-scale unchanged-input refresh: " + shown(steady.get('wall_seconds'),' seconds') +
            "; Gold inserts/updates/deletes: " + str(steady.get('gold_rows_inserted')) + "/" +
            str(steady.get('gold_rows_updated')) + "/" + str(steady.get('gold_rows_deleted')) +
            ". This measures Gold steady-state work with no newly landed batch, not full "
            "weekday ingestion.","MPSmall")
    add("The implemented platform is Databricks Free Edition on AWS, rather than a measured "
        "Azure for Students deployment. A confirmed Free Edition bill can be zero while compute "
        "quota is still consumed. No Azure credit usage or billed DBUs are invented. Wall time "
        "and summed step durations are operational measurements, not interchangeable with billed "
        "compute. Capture a normal daily run separately from full-load and rerun demos.")
    sub("Enterprise hourly scenario")
    add("Hourly 24/7 operation implies <b>8,760 runs per year</b>. Annual estimate = "
        "8,760 x enterprise per-run cost + 12 x (monthly storage + BI + other recurring costs). "
        "Per-run cost must reflect DBU/SKU price and separately billed VMs when relevant, plus "
        "startup, minimum billing, idle capacity and retries. Serverless infrastructure must "
        "not be counted twice.")
    ready=all(numeric(fin.get(k)) for k in ('enterprise_cost_per_run_usd','storage_monthly_usd','bi_monthly_usd','other_monthly_usd'))
    annual=8760*fin['enterprise_cost_per_run_usd']+12*sum(fin[k] for k in ('storage_monthly_usd','bi_monthly_usd','other_monthly_usd')) if ready else None
    table(["Enterprise assumption","Value"],[
        ["Region", fin.get('enterprise_region') or "Not supplied"],
        ["Estimated cost per run", shown(fin.get('enterprise_cost_per_run_usd'),' USD')],
        ["Estimated annual architectural cost", shown(annual,' USD')],
    ],[240,WIDTH-240])
    if fin.get('assumptions'):
        add(escape(fin['assumptions']),"MPSmall")
    add("Cost improvements: process after daily source updates; use sampled development data; "
        "terminate idle warehouses; reuse materialized stages; tune Import refresh frequency; "
        "consider affected-ticker recalculation only if measured full-history cost warrants it.","MPSmall")

    page(9,"Power BI and submission readiness")
    bi=e.get("power_bi",{})
    figure(bi.get('dashboard_image'),"Saad's dashboard screenshot pending: price + MA7/30, relative volatility, volume versus absolute return, and pipeline/error visuals.",185)
    figure(bi.get('model_image'),"Saad's Model-view screenshot pending: one-to-many single-direction relationships from Date and Ticker to facts.",150)
    add("Power BI Desktop connects directly to the Databricks SQL warehouse using Import "
        "mode. A single-ticker price chart avoids summing company prices. Risk comparisons "
        "use each ticker's latest available 30-return volatility within the selected dates; "
        "tooltips expose stale observation dates. Scatter points retain ticker-date grain.")
    add("Operations show execution status, duration, actual quarantine reasons and revision "
        "counts. Summing rejected-row counters across multiple logged steps would double-count "
        "rejects; the quarantine view supplies those totals instead.")
    add("Final deliverables are the updated GitHub repository, Saad's .pbix and this report "
        "with real evidence. The checked-in handoff guide lists the exact connections, measures "
        "and screenshots. Draft status is removed only after finalization checks pass.","MPSmall")

    page(10,"References and evidence register")
    references=[
        ("Project source and version history","https://github.com/Soban06/MarketPulse"),
        ("Databricks constraint semantics","https://docs.databricks.com/aws/en/tables/constraints"),
        ("Databricks partitioning guidance","https://docs.databricks.com/aws/en/tables/partitions"),
        ("Serverless limitations / Spark UI","https://docs.databricks.com/aws/en/compute/serverless/limitations"),
        ("Query Profile","https://docs.databricks.com/aws/en/sql/user/queries/query-profile"),
        ("Power BI Databricks connector","https://learn.microsoft.com/en-us/power-query/connectors/databricks"),
        ("Serverless bundle configuration","https://docs.databricks.com/aws/en/dev-tools/bundles/examples"),
    ]
    for title,url in references:
        add(f'<b>{title}</b><br/><link href="{escape(url)}" color="#087F8C">{escape(url)}</link>',"MPSmall")
    if fin.get('pricing_source'):
        add("<b>Scenario pricing source:</b> " + escape(fin['pricing_source']),"MPSmall")
    sub("Repository evidence")
    table(["Evidence","Location / meaning"],[
        ["Original business questions", "Phase 1 proposal supplied by Soban; three questions retained."],
        ["Phase 2 saved runs", "docs/run-evidence/80, 81, 90 and 91 HTML exports; historical evidence."],
        ["Gold integration", "notebooks/82_test_gold.py; export actual outputs after workspace pass."],
        ["DAG / runtime / layout", "notebooks/93_phase3_evidence.py; actual measurements and screenshots."],
        ["Metric and BI contract", "docs/phase3/GOLD_MODEL.md, POWER_BI_HANDOFF.md, bi/measures.dax."],
        ["Report input evidence", "docs/report/evidence.json; no invented results permitted."],
    ],[145,WIDTH-145])
    missing=missing_evidence(e)
    if missing:
        sub("Still required before final submission")
        add(escape("; ".join(missing)) + ".","MPSmall")
    else:
        add("All automated evidence-presence checks passed. Review the real screenshots, "
            "assumptions and deliverables before submission.","MPSmall")

    out=ROOT / "output/pdf" / ("MarketPulse_Architecture_Report.pdf" if final else "MarketPulse_Architecture_Report_DRAFT.pdf")
    out.parent.mkdir(parents=True,exist_ok=True)
    doc=SimpleDocTemplate(str(out),pagesize=A4,rightMargin=48,leftMargin=48,
                          topMargin=48,bottomMargin=48,title="MarketPulse Architecture and BI Report",
                          author="Soban; Power BI: Saad Jahangir")
    def footer(canvas, doc):
        canvas.saveState(); canvas.setStrokeColor(TEAL); canvas.setLineWidth(1)
        canvas.line(48,38,A4[0]-48,38); canvas.setFont("Helvetica",8); canvas.setFillColor(MUTED)
        canvas.drawString(48,25,"MARKETPULSE / PHASE 3" + (" / DRAFT" if not final else ""))
        canvas.drawRightString(A4[0]-48,25,f"{doc.page}"); canvas.restoreState()
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    return out


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final',action='store_true')
    parser.add_argument('--evidence',type=Path,default=ROOT/'docs/report/evidence.json')
    args=parser.parse_args()
    evidence=json.loads(args.evidence.read_text(encoding='utf-8'))
    missing=missing_evidence(evidence)
    if args.final and missing:
        parser.error("Final report blocked by missing real evidence:\n- " + "\n- ".join(missing))
    print(build(evidence,final=args.final))
    if missing:
        print(f"DRAFT: {len(missing)} evidence requirements still need real workspace/BI results.")


if __name__=='__main__':
    main()
