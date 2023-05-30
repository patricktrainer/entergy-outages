from flask import Flask, render_template, jsonify
import sqlite3
import pathlib

app = Flask(__name__)

def get_db():
    db_path = pathlib.Path(__file__).parent.absolute() / "entergy_outages.db"
    db = sqlite3.connect(db_path)
    return db

def get_db_connection():
    conn = get_db()
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/outages')
def api_commits():
    conn = get_db_connection()
    commits = conn.execute('SELECT * FROM outages limit 10').fetchall()
    commit_list = [dict(commit) for commit in commits]
    return jsonify(commit_list)


if __name__ == '__main__':
    app.run(debug=True)
