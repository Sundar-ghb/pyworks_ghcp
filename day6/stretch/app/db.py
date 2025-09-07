import duckdb

def get_connection():
    return duckdb.connect(database=':memory:')  # or 'data.duckdb'

def store_result(text: str, result: dict):
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inference_results (
            text STRING,
            label STRING,
            score DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO inference_results VALUES (?, ?, ?)
    """, (text, result['label'], result['score']))
    conn.close()