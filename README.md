
# **Data-Driven Trading Strategy Research on FIFA Market**

## Table of Contents

- [**Data-Driven Trading Strategy Research on FIFA Market**](#data-driven-trading-strategy-research-on-fifa-market)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [**Data Scraping**](#data-scraping)
    - [Deployed Data Scraper Retrieve Steps](#deployed-data-scraper-retrieve-steps)
    - [Features](#features)
    - [Database Structure](#database-structure)
  - [**Feature Engineering**](#feature-engineering)
  - [**ML Model Training**](#ml-model-training)
  - [**Strategy Design**](#strategy-design)
  - [**Backtesting \& Simulation**](#backtesting--simulation)
    - [Backtesting on Dip\_Rebound Strategy](#backtesting-on-dip_rebound-strategy)
  - [**Live Execution**](#live-execution)
  - [Setup](#setup)
  - [Usage](#usage)
  - [Contributing](#contributing)
  - [License](#license)

---

## Overview

Briefly describe your project:

> This project collects and analyzes FIFA Ultimate Team (FUT) player market data to research and test trading strategies. It stores player stats, prices, playstyles, and historical trends in a PostgreSQL database.

## **Data Scraping**

---

### Deployed Data Scraper Retrieve Steps

1. Open PuTTy and load fifa settings
2. extract data on fc26_data.ipynb

/home/Shaninho/fifa_market_strategiser/venv/bin/python /home/Shaninho/fifa_market_strategiser/main_scraper.py

![alt text](png/autoscraping.png)

### Features
![Players Database](png/FUTBINplayers.png)
![Player Details](png/playerdetails.png)
![Player Value](png/playervalue.png)
* Stores player attributes, stats, and historical price data.
* Tracks player playstyles and roles.
* Maintains pace, shooting, passing, dribbling, defending, and physical statistics.
* Supports historical tracking of player market values (PC and console).

---

### Database Structure

Database tables:

* **players** – Basic player info (name, rating, height, acceleration, etc.)
* **price\_history** – Tracks PC and console prices over time.
* **player\_playstyles** – Stores each player’s playstyle(s) and level.
* **player\_roles** – Stores positions and roles for each player.
* **player\_pace\_stats** – Pace-specific stats.
* **player\_shooting\_stats** – Shooting-specific stats.
* **player\_passing\_stats** – Passing-specific stats.
* **player\_dribbling\_stats** – Dribbling-specific stats.
* **player\_defending\_stats** – Defending-specific stats.
* **player\_physical\_stats** – Physical stats.

![Diagram](png/Untitled(9).png)

---

## **Feature Engineering**

Scatter Plot of 82 Rated Player Prices
![82Players](png/82_prices.png)

## **ML Model Training**
## **Strategy Design**
## **Backtesting & Simulation**

### Backtesting on Dip_Rebound Strategy

| Card                                | Buy Price | Buy Time            | Sell Price | Sell Time           | Net Profit |
|------------------------------------|-----------|-------------------|------------|-------------------|------------|
| Laura Georges                       | 225,000   | 2025-09-23 07:00  | 273,000    | 2025-09-23 11:00  | 34,350     |
| Alphonso Davies                     | 24,000    | 2025-09-23 15:00  | 27,500     | 2025-09-23 20:00  | 2,125      |
| Debora C. de Oliveira               | 48,250    | 2025-09-23 21:00  | 51,000     | 2025-09-24 00:00  | 200        |
| Frank Lampard                        | 269,000   | 2025-09-23 20:00  | 298,000    | 2025-09-24 01:00  | 14,100     |
| Freddie Ljungberg                    | 114,000   | 2025-09-23 16:00  | 129,000    | 2025-09-24 02:00  | 8,550      |
| Jude Bellingham                       | 185,000   | 2025-09-24 03:00  | 209,000    | 2025-09-24 06:00  | 13,550     |
| Theo Hernandez                        | 62,000    | 2025-09-23 03:00  | 55,000     | 2025-09-24 09:00  | -9,750     |
| Gennaro Gattuso                        | 170,000   | 2025-09-24 07:00  | 190,000    | 2025-09-24 10:00  | 10,500     |
| David Beckham                          | 461,000   | 2025-09-24 05:00  | 510,000    | 2025-09-24 20:00  | 23,500     |
| Bruno Miguel Borges Fernandes          | 10,000    | 2025-09-24 20:00  | 13,250     | 2025-09-24 23:00  | 2,587.5    |
| Tijjani Reijnders                      | 134,000   | 2025-09-24 13:00  | 145,000    | 2025-09-25 00:00  | 3,750      |
| Dirk Kuyt                              | 62,000    | 2025-09-24 18:00  | 70,000     | 2025-09-25 03:00  | 4,500      |
| Bixente Lizarazu                        | 250,000   | 2025-09-24 16:00  | 291,000    | 2025-09-25 05:00  | 26,450     |
| Clint Dempsey                          | 31,000    | 2025-09-25 03:00  | 39,000     | 2025-09-25 06:00  | 6,050      |
| Javier Zanetti                          | 319,000   | 2025-09-24 19:00  | 387,000    | 2025-09-25 07:00  | 48,650     |
| Nwankwo Kanu                            | 28,250    | 2025-09-25 11:00  | 34,000     | 2025-09-25 14:00  | 4,050      |
| Fernando Hierro Ruiz                     | 260,000   | 2025-09-25 05:00  | 290,000    | 2025-09-25 16:00  | 15,500     |
| Miroslav Klose                           | 347,000   | 2025-09-25 02:00  | 399,000    | 2025-09-25 17:00  | 32,050     |
| Steven Gerrard                           | 356,000   | 2025-09-25 09:00  | 400,000    | 2025-09-25 17:00  | 24,000     |
| Jerzy Dudek                              | 65,000    | 2025-09-24 21:00  | 72,500     | 2025-09-25 18:00  | 3,875      |
| Antonio Rudiger                           | 66,500    | 2025-09-25 12:00  | 73,500     | 2025-09-25 19:00  | 3,325      |
| Giorgi Mamardashvili                      | 13,000    | 2025-09-25 16:00  | 15,250     | 2025-09-25 21:00  | 1,487.5    |
| Ashley Cole                               | 410,000   | 2025-09-25 07:00  | 470,000    | 2025-09-25 22:00  | 36,500     |
| Robert Pires                              | 240,000   | 2025-09-25 15:00  | 267,000    | 2025-09-26 06:00  | 13,650     |
| Ruud van Nistelrooy                       | 190,000   | 2025-09-26 05:00  | 215,000    | 2025-09-26 08:00  | 14,250     |
| Ricardo Alberto Silveira Carvalho         | 215,000   | 2025-09-26 06:00  | 255,000    | 2025-09-26 09:00  | 27,250     |
| Cole Palmer                               | 40,750    | 2025-09-26 08:00  | 45,000     | 2025-09-26 12:00  | 2,000      |
| Ronald Koeman                             | 230,000   | 2025-09-26 03:00  | 255,000    | 2025-09-26 13:00  | 12,250     |
| Alessia Russo                             | 14,500    | 2025-09-26 13:00  | 16,500     | 2025-09-26 17:00  | 1,175      |
| Joan Capdevila Mendez                     | 200,000   | 2025-09-26 14:00  | 222,000    | 2025-09-26 18:00  | 10,900     |
| Tim Cahill                                | 35,000    | 2025-09-26 16:00  | 41,000     | 2025-09-26 19:00  | 3,950      |
| Bruno Miguel Borges Fernandes             | 10,000    | 2025-09-26 18:00  | 11,000     | 2025-09-26 21:00  | 450        |
| Alexander Isak                            | 147,000   | 2025-09-26 07:00  | 159,000    | 2025-09-27 00:00  | 4,050      |
| Paul Scholes                              | 180,000   | 2025-09-26 10:00  | 205,000    | 2025-09-27 02:00  | 14,750     |
| Gianluca Vialli                           | 102,000   | 2025-09-26 15:00  | 115,000    | 2025-09-27 02:00  | 7,250      |
| Steve McManaman                            | 125,000   | 2025-09-26 22:00  | 138,000    | 2025-09-27 02:00  | 6,100      |
| Jude Bellingham                            | 165,000   | 2025-09-27 02:00  | 185,000    | 2025-09-27 05:00  | 10,750     |
| Sami Al Jaber                              | 64,500    | 2025-09-26 23:00  | 73,500     | 2025-09-27 08:00  | 5,325      |
| Ruud van Nistelrooy                        | 189,000   | 2025-09-27 09:00  | 214,000    | 2025-09-27 12:00  | 14,300     |
| Bukayo Saka                                | 14,000    | 2025-09-27 15:00  | 16,000     | 2025-09-27 18:00  | 1,200      |
| Marco van Basten                           | 420,000   | 2025-09-25 22:00  | 450,000    | 2025-09-27 19:00  | 7,500      |
| Henrik Larsson                             | 160,000   | 2025-09-27 07:00  | 179,000    | 2025-09-27 19:00  | 10,050     |
| Ian Rush                                   | 125,000   | 2025-09-27 11:00  | 140,000    | 2025-09-27 22:00  | 8,000      |
| Willian Pacho                              | 109,000   | 2025-09-27 20:00  | 120,000    | 2025-09-27 23:00  | 5,000      |
| Harry Kane                                 | 17,500    | 2025-09-27 14:00  | 19,500     | 2025-09-28 01:00  | 1,025      |
| Thibaut Courtois                           | 60,000    | 2025-09-27 13:00  | 68,000     | 2025-09-28 02:00  | 4,600      |
| Hernan Crespo                              | 175,000   | 2025-09-27 01:00  | 200,000    | 2025-09-28 07:00  | 15,000     |
| Marcos Llorente Moreno                      | 75,000    | 2025-09-28 06:00  | 84,500     | 2025-09-28 09:00  | 5,275      |
| Tim Cahill                                 | 35,000    | 2025-09-28 11:00  | 38,500     | 2025-09-28 14:00  | 1,575      |
| Fara Williams                              | 90,000    | 2025-09-28 05:00  | 100,000    | 2025-09-28 15:00  | 5,000      |

**Total Profit:** 555,237.5

## **Live Execution**

![alt text](png/discordbot.png)

Implemented a discord bot for live deal alerts to compliment the trading strategies from live data


## Setup

Explain how to set up the project locally:

1. Clone the repository:

   ```bash
   git clone https://github.com/Shaninhooo/Data-Driven-Trading-Strategy-Research-on-FIFA-Market.git
   ```
2. Create a `.env` file with your PostgreSQL credentials:

   ```env
   DB_NAME=your_db_name
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432
   ```
3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
4. Run the Python script to initialize the database tables:

   ```bash
   python postgresConnect.py
   ```

---

## Usage

Explain how someone can use your database or scripts:

* Insert new player data.
* Query historical price trends.
* Analyze player statistics for trading strategies.

Example:

```python
cur.execute("SELECT * FROM price_history WHERE player_id = 1;")
```

---

## Contributing

> Optional section for collaborators.

* Fork the repo, create a branch, submit a pull request.

---

## License

> Optional, but recommended (MIT, Apache, etc.)

---

