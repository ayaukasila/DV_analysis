import time
import random
import psycopg2
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# Настройки подключения к базе
DSN = "host=localhost port=5432 dbname=e-commerce user=postgres password=0000"

def next_month(d: date) -> date:
    """Возвращает 1-е число следующего месяца"""
    return d.replace(day=1) + relativedelta(months=1)

def main():
    print("🚀 Auto-refresh CSV started (writing to public.orders_monthly_csv)")

    with psycopg2.connect(DSN) as conn:
        conn.autocommit = True
        cur = conn.cursor()

        # Создаём таблицу, если её нет
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.orders_monthly_csv (
            month date PRIMARY KEY,
            orders_count int,
            inserted_at timestamptz DEFAULT now()
        );
        """)

        # Убедимся, что есть уникальный индекс (для ON CONFLICT)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_monthly_csv_month
        ON public.orders_monthly_csv (month);
        """)

        # Берём последний месяц
        cur.execute("""
            SELECT COALESCE(MAX(month::date), DATE '2016-08-01')
            FROM public.orders_monthly_csv;
        """)
        last_month = cur.fetchone()[0]
        if isinstance(last_month, str):
            last_month = date.fromisoformat(last_month)

        # Берём последнее значение orders_count
        cur.execute("""
            SELECT orders_count
            FROM public.orders_monthly_csv
            WHERE month = (SELECT MAX(month) FROM public.orders_monthly_csv);
        """)
        row = cur.fetchone()
        base = row[0] if row and row[0] is not None else 300

        counter = 1
        while True:
            # Следующий месяц
            m = next_month(last_month)
            delta = random.randint(-250, 350)
            value = max(0, base + delta)

            # Вставляем данные или обновляем при совпадении месяца
            cur.execute("""
                INSERT INTO public.orders_monthly_csv (month, orders_count, inserted_at)
                VALUES (%s, %s, now())
                ON CONFLICT (month)
                DO UPDATE SET
                    orders_count = EXCLUDED.orders_count,
                    inserted_at  = now();
            """, (m, int(value)))

            print(f"✅ CSV+ #{counter}: {m.isoformat()} → {value} (inserted_at={datetime.now().isoformat()})")

            last_month = m
            base = value
            counter += 1

            # интервал вставки
            time.sleep(8)

if __name__ == "__main__":
    main()
