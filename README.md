# 🚀 DataPurge Pro

DataPurge Pro is a high-performance, memory-safe CLI data engineering tool designed to validate, stream, and clean multi-gigabyte JSON and CSV files with **zero RAM bloat**. 

Built entirely in Python using a line-by-line generator/streaming approach, it can process massive datasets using less than 100MB of RAM, making it completely Out-Of-Memory (OOM) safe.

---
## 📸 Screenshots

![DataPurge Pro Interface 1](1 loading screen.png.png)

![DataPurge Pro Interface 2](2 results.png.png)

![DataPurge Pro Interface 3](3 results.png.png)

## ⚡ Performance Benchmark
- **Dataset Size:** 100,000 rows (2.6 MB Data Log)
- **Processing Time:** **0.23 seconds**
- **Memory Footprint:** < 50MB RAM

---

## ✨ Key Features
- **Memory Efficient:** Streams gigabyte-scale files effortlessly without loading the entire file into memory.
- **Comprehensive Validation:** Automatically catches malformed rows, mismatched columns, missing fields, and JSON syntax errors.
- **Beautiful TUI:** Integrated with the `rich` library for dynamic, real-time progress bars and gorgeous terminal report tables.
- **Auto-Generated Reports:** Creates a standalone Markdown report (`_report.md`) next to your input file automatically, detailing every single error with its exact line number.

---

## 📦 Installation & Setup

Follow these simple steps to get DataPurge Pro running on your machine:

### 1. Prerequisites
Make sure you have Python 3.11+ installed on your system.

### 2. Install Dependencies
DataPurge Pro relies on the `rich` library for its beautiful Terminal User Interface. Run the following command in your terminal:
```bash
pip install rich
