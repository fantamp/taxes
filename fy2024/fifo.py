import pandas as pd
from collections import deque

portfolio_path = "/Users/fantamp/Library/Mobile Documents/com~apple~CloudDocs/Streams/Финансы 💰/Налоги/Налоги 2025 (FY24)/Yandex_Nebius Options/PortfolioDetails _5503097.xlsx"
transactions_path = "/Users/fantamp/Library/Mobile Documents/com~apple~CloudDocs/Streams/Финансы 💰/Налоги/Налоги 2025 (FY24)/Yandex_Nebius Options/CompletedTransactions_5503097.xlsx"

# Загрузка данных
portfolio_df_full = pd.read_excel(
    portfolio_path, sheet_name="Portfolio details", skiprows=5
)
transactions_df = pd.read_excel(
    transactions_path, sheet_name="Completed transactions", skiprows=3
)

# Очистка и структурирование данных портфеля
portfolio_df_full.columns = portfolio_df_full.columns.str.strip()
portfolio_df_full = portfolio_df_full[
    ["Allocation date", "Allocated quantity", "Market price"]
]
portfolio_df_full.dropna(subset=["Allocated quantity"], inplace=True)
portfolio_df_full["Allocation date"] = pd.to_datetime(
    portfolio_df_full["Allocation date"]
)
portfolio_df_full["Allocated quantity"] = pd.to_numeric(
    portfolio_df_full["Allocated quantity"]
)
portfolio_df_full["Market price"] = pd.to_numeric(portfolio_df_full["Market price"])

# Агрегация грантов по дате
aggregated_grants = (
    portfolio_df_full.groupby("Allocation date", as_index=False)
    .agg({"Allocated quantity": "sum", "Market price": "mean"})
    .sort_values(by="Allocation date")
)

# FIFO-очередь
fifo_queue = deque()
fifo_balance = 0.0
fifo_history = []

# Добавление грантов в FIFO-очередь
for _, grant in aggregated_grants.iterrows():
    fifo_queue.append(
        {
            "grant_date": grant["Allocation date"],
            "quantity": grant["Allocated quantity"],
            "grant_price": grant["Market price"],
        }
    )
    fifo_balance += grant["Allocated quantity"]
    fifo_history.append(
        {
            "event_date": grant["Allocation date"],
            "event_type": "grant",
            "grant_date": grant["Allocation date"],
            "change": grant["Allocated quantity"],
            "grant_price": grant["Market price"],
            "sale_price": None,
            "gain": None,
            "fifo_balance": fifo_balance,
        }
    )

# Обработка операций продаж и forfeiture
sales_df = transactions_df[
    transactions_df["Order type"].str.contains("Exercise-and-sell", na=False)
].copy()
sales_df["Date"] = pd.to_datetime(sales_df["Date"])
sales_df["Quantity"] = pd.to_numeric(sales_df["Quantity"])
sales_df["Execution price"] = pd.to_numeric(
    sales_df["Execution price"], errors="coerce"
)
sales_df.dropna(subset=["Execution price"], inplace=True)

# События продаж
sales_events = sales_df[["Date", "Quantity", "Execution price"]].rename(
    columns={
        "Date": "event_date",
        "Quantity": "change",
        "Execution price": "sale_price",
    }
)
sales_events["change"] = -sales_events["change"]
sales_events["type"] = "sale"

# События forfeiture (автоматически из таблицы)
forfeitures_df = transactions_df[transactions_df["Order type"] == "Forfeiture"].copy()
forfeitures_df["Date"] = pd.to_datetime(forfeitures_df["Date"])
forfeitures_df["Quantity"] = pd.to_numeric(forfeitures_df["Quantity"])

forfeitures_events = forfeitures_df[["Date", "Quantity"]].rename(
    columns={"Date": "event_date", "Quantity": "change"}
)
forfeitures_events["change"] = -forfeitures_events["change"]
forfeitures_events["type"] = "forfeiture"
forfeitures_events["sale_price"] = None

# Применение списаний FIFO
for _, event in (
    pd.concat([sales_events, forfeitures_events])
    .sort_values(by="event_date")
    .iterrows()
):
    quantity_to_process = -event["change"]
    event_type = event["type"]
    sale_price = event.get("sale_price", None)

    while quantity_to_process > 0 and fifo_queue:
        grant = fifo_queue[0]
        available_qty = grant["quantity"]
        qty_processed = min(quantity_to_process, available_qty)

        gain = None
        if sale_price is not None:
            gain = (sale_price - grant["grant_price"]) * qty_processed

        fifo_history.append(
            {
                "event_date": event["event_date"],
                "event_type": event_type,
                "grant_date": grant["grant_date"],
                "change": -qty_processed,
                "grant_price": grant["grant_price"],
                "sale_price": sale_price,
                "gain": gain,
                "fifo_balance": sum(item["quantity"] for item in fifo_queue)
                - qty_processed,
            }
        )

        quantity_to_process -= qty_processed
        grant["quantity"] -= qty_processed

        if grant["quantity"] == 0:
            fifo_queue.popleft()

# Итоговая таблица
fifo_history_df = pd.DataFrame(fifo_history)
fifo_history_df.sort_values(by="event_date", inplace=True)
fifo_history_df.reset_index(drop=True, inplace=True)

# Пересчёт FIFO Balance отдельным проходом после сортировки
fifo_balance = 0
for idx, row in fifo_history_df.iterrows():
    fifo_balance += row["change"]
    fifo_history_df.at[idx, "fifo_balance"] = fifo_balance
    fifo_history_df["market_sum"] = fifo_history_df["change"] * fifo_history_df["grant_price"]

# Красивый вывод в консоль и сохранение в CSV
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_rows", None)

print(fifo_history_df)

fifo_history_df.to_csv("fifo_history.csv", index=False)
