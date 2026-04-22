# 999.md Scraper

An automated web scraping and data extraction tool to identify the best vehicle deals from 999.md.

## Why I Built This
I was searching for a car and found it impossible to manually track all the listings, compare prices, and identify the truly good deals among thousands of vehicles. I needed a way to pull all the data into one place for objective analysis.

This tool runs locally, launching headless browser threads to bypass protections and scrape vehicle data rapidly. It extracts the title, price, mileage, year, and technical specifications, saving them into CSV or JSON formats. It also provides an interactive local dashboard where you can set complex filters and monitor the scraping progress live.

It was an excellent technical challenge to learn about Python multi-threading, Playwright for dynamic content extraction, and building a local web API to communicate with a frontend dashboard.

## Features
- **Parallel Extraction**: High-speed vehicle data extraction using headless browsers.
- **Advanced Filtering**: Filter by price, year, mileage, fuel type, transmission, and body type before extraction.
- **Live Dashboard**: Real-time progress tracking, live logging, and instant statistics.
- **Data Analytics**: Visual charts for makes, fuel types, years, and prices.
- **Historical Database**: Automatically saves previous extractions for quick reference.

## Try It Out
View the project dashboard interface here: [999.md Scraper Dashboard](https://iulianplop1.github.io/Scrapper/)
*(Note: To actually extract data, the Python backend server must be run locally)*

Created by Iulian Plop
