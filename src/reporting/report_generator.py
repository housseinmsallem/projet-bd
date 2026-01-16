import os
import datetime

# Try to import FPDF
try:
    from fpdf import FPDF
    HAS_FPDF = True
except ImportError:
    HAS_FPDF = False
    FPDF = object # dummy

class PDFReport(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'YouTube Data Analysis Project: Israel-Palestine Conflict', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, 'Page ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 6, title, 0, 1, 'L', 1)
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 11)
        self.multi_cell(0, 5, body)
        self.ln()

    def add_image(self, image_path, title=''):
        if os.path.exists(image_path):
            self.add_page()
            self.chapter_title(title)
            self.image(image_path, x=10, y=None, w=190)
        else:
            print(f"Image not found: {image_path}")

class ReportGenerator:
    def __init__(self, output_dir="."):
        self.output_dir = output_dir
        
    def generate_pdf(self, data, charts, filename="final_report.pdf"):
        if not HAS_FPDF:
            print("FPDF library not found. Cannot generate PDF.")
            return False
            
        pdf = PDFReport()
        pdf.alias_nb_pages()
        pdf.add_page()
        
        # Executive Summary
        pdf.chapter_title("1. Executive Summary")
        pdf.chapter_body(data.get('summary', 'No summary provided.'))
        
        # Methodology
        pdf.chapter_title("2. Methodology")
        pdf.chapter_body(data.get('methodology', 
            "Data collected via YouTube API. Processed using Apache Spark. \n"
            "Analysis includes Sentiment Analysis, Topic Modeling (LDA), and Temporal Analysis."
        ))
        
        # Architecture
        pdf.chapter_title("3. Data Pipeline Architecture")
        pdf.chapter_body(
            "1. Collection: Python script -> YouTube Data API -> JSON/HDFS\n"
            "2. Storage: HDFS (Raw Data)\n"
            "3. Processing: PySpark (Cleaning, NLP, Aggregation)\n"
            "4. Analysis: TextBlob (Sentiment), Spark ML (Topics)\n"
            "5. Visualization: Matplotlib/Seaborn"
        )
        
        # Key Findings
        pdf.chapter_title("4. Key Findings")
        pdf.chapter_body(data.get('findings', 'Analysis pending.'))
        
        # Visualizations (New Pages)
        for chart_title, chart_path in charts.items():
            pdf.add_image(chart_path, chart_title)
            
        # Stance Analysis (Specific Section)
        pdf.add_page()
        pdf.chapter_title("5. Stance & Engagement Analysis")
        pdf.chapter_body(data.get('stance_analysis', 
            "Stance detection based on keyword heuristics shows the evolution of public opinion.\n"
            "Peaks typically correlate with major conflict events."
        ))

        # Conclusions
        pdf.add_page()
        pdf.chapter_title("6. Conclusions & Limitations")
        pdf.chapter_body(data.get('conclusion', 
            "Analysis suggests significant engagement spikes during conflict escalations.\n"
            "Limitations: API quota limits, English-only analysis, and potential bot activity."
        ))
        
        out_path = os.path.join(self.output_dir, filename)
        pdf.output(out_path, 'F')
        print(f"PDF Report generated: {out_path}")
        return True

    def generate_markdown(self, data, charts, filename="final_report.md"):
        content = f"""# YouTube Data Analysis: Israel-Palestine Conflict
**Date:** {datetime.datetime.now().strftime('%Y-%m-%d')}

## 1. Executive Summary
{data.get('summary', 'No summary provided.')}

## 2. Methodology
{data.get('methodology', 'Standard pipeline.')}

## 3. Data Pipeline Architecture
- Collection: YouTube Data API
- Storage: HDFS
- Processing: Apache Spark
- Analysis: NLP & Sentiment

## 4. Key Findings
{data.get('findings', 'Pending analysis.')}

## 5. Visualizations
"""
        for title, path in charts.items():
            content += f"### {title}\n![{title}]({path})\n\n"
            
        content += f"""
## 6. Conclusions & Limitations
{data.get('conclusion', 'Pending conclusion.')}
"""
        out_path = os.path.join(self.output_dir, filename)
        with open(out_path, 'w') as f:
            f.write(content)
        print(f"Markdown Report generated: {out_path}")
        return True
