# 🧠 Social Listening Tool for Strategic Consultancy

**Cloud-Native, Low-Code, NLP-Powered Platform for Extracting Insights from Social Media**

---

## 🔗 Live Demo & Full Project Walkthrough

📺 **Explore more visuals, architecture breakdown, and a detailed video walkthrough on my portfolio**
👉 [https://prathamskk.github.io/portfolio/slt/](https://prathamskk.github.io/portfolio/slt/)

---

## 📸 Screenshots

> From query to insight – zero code required.

**Control Panel (Google Sheets UI)**
![Control Centre](https://prathamskk.github.io/portfolio/slt_1.png)

**Interactive Dashboards (Looker Studio)**
![Dashboard 1](https://prathamskk.github.io/portfolio/slt_2.png)
![Dashboard 2](https://prathamskk.github.io/portfolio/slt_3.png)
![Dashboard 3](https://prathamskk.github.io/portfolio/slt_4.png)

---

## 📌 Project Overview

This tool was built as part of an MSc Business Analytics dissertation with **Sense Worldwide**. It enables **non-technical consultants** to generate actionable insights from Reddit and Quora posts using automated scraping, machine learning, and large language models — all via a low-code Google Sheets interface.

---

## 🧱 Architecture Overview

```
Google Sheets → Brightdata → Pub/Sub → Cloud Functions → BigQuery ML + Vertex AI → Looker Studio
```

**Key Layers**:

* **UI**: Google Sheets (low-code control panel)
* **Ingestion**: Brightdata (SERP + scraper APIs)
* **Orchestration**: GCP Cloud Functions + Pub/Sub
* **ML/NLP**: BigQuery ML (K-Means), Vertex AI (embeddings + Gemini), Cloud NLP API
* **Visualization**: Looker Studio dashboards

---

## 📁 Project Structure

```bash
SOCIAL-LISTENING-TOOL-DISSERTATION/
├── bigquery/            # SQL models for clustering, sentiment, embedding, UMAP
├── cloud_functions/     # Python functions for scraping and ML pipeline
├── googleappscript/     # Apps Script UI logic (Sheets-based control)
└── .gitattributes
```

---

## 🔧 Key Features

* 🔍 **SERP-powered discovery** + scraping of Reddit/Quora
* 🧠 **Text embeddings + K-Means** topic modeling
* 💬 **LLM topic summaries** via Gemini (Vertex AI)
* ❤️ **Sentiment scoring** with Cloud NLP API
* 📊 **Dashboard output** with trends, clusters, insights
* 🧾 **Low-code workflow** for consultants, no setup required

---

## 🚀 Setup Guide

1. **Clone the repo**

```bash
git clone https://github.com/yourusername/social-listening-tool.git
cd social-listening-tool
```

2. **Enable GCP APIs**

* BigQuery
* Cloud Functions
* Pub/Sub
* Vertex AI
* Cloud Natural Language API

3. **Deploy Functions & SQL Models**
   Deploy Python functions from `cloud_functions/` and run SQL in `bigquery/`.

4. **Configure Google Sheets UI**
   Import `googleappscript/` files into Apps Script and bind to a Sheet.

---