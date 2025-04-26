from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import sql
import os

app = Flask(__name__)

#Database connection details (using envionment variables in Render
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_PASS = os.getenv("DB_PASS")

#Function to connect to the database
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

#API endpoint to execute SQL queries
@app.route('/query', methods=['POST'])
def execute_query():
    try:
        data = request.get_json()
        query = data.get('query')
        if not query:
            return jsonify({"error": "No query provided"}), 400

        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500

        cur = conn.cursor()
        cur.execute(query)
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            results = cur.fetchall()
            response = [dict(zip(columns, row)) for row in results]
        else:
            conn.commit()
            response = {"message": "Query executed successfully", "rows_affected": cur.rowcount}

        cur.close()
        conn.close()
        return jsonify(response), 200

    except Exception as e:
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)