from flask import Flask, render_template, jsonify
import pymysql
import pymysql.cursors
from collections import defaultdict

app = Flask(__name__)

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'social_analysis',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/hot-products')
def hot_products():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT window_start, window_end, item_id, total_hot_score
            FROM hot_products
            ORDER BY window_start DESC, total_hot_score DESC
            LIMIT 50
        """)
        data = cursor.fetchall()
    conn.close()
    return jsonify(data)

@app.route('/api/behavior-stats')
def behavior_stats():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT window_start, window_end, behavior_type, total_count
            FROM behavior_stats
            ORDER BY window_start DESC
            LIMIT 200
        """)
        data = cursor.fetchall()
    conn.close()
    return jsonify(data)

@app.route('/api/behavior-trend')
def behavior_trend():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT window_start, behavior_type, total_count
            FROM behavior_stats
            ORDER BY window_start DESC
            LIMIT 200
        """)
        rows = cursor.fetchall()
    conn.close()

    trend_data = defaultdict(lambda: {'interaction': 0, 'purchase': 0, 'view': 0})
    for row in rows:
        ws = row['window_start'].strftime('%Y-%m-%d %H:%M:%S')
        bt = row['behavior_type']
        trend_data[ws][bt] = row['total_count']
    
    sorted_windows = sorted(trend_data.keys(), reverse=False)
    result = []
    for ws in sorted_windows:
        result.append({
            'window': ws,
            'interaction': trend_data[ws]['interaction'],
            'purchase': trend_data[ws]['purchase'],
            'view': trend_data[ws]['view']
        })
    return jsonify(result)

@app.route('/api/kpi')
def kpi():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT SUM(total_count) as total_interaction 
            FROM behavior_stats 
            WHERE behavior_type='interaction' 
            ORDER BY window_start DESC 
            LIMIT 1
        """)
        interaction = cursor.fetchone()['total_interaction'] or 0
        
        cursor.execute("""
            SELECT SUM(total_count) as total_purchase 
            FROM behavior_stats 
            WHERE behavior_type='purchase' 
            ORDER BY window_start DESC 
            LIMIT 1
        """)
        purchase = cursor.fetchone()['total_purchase'] or 0
        
        cursor.execute("""
            SELECT SUM(total_count) as total_view 
            FROM behavior_stats 
            WHERE behavior_type='view' 
            ORDER BY window_start DESC 
            LIMIT 1
        """)
        view = cursor.fetchone()['total_view'] or 0
    conn.close()
    return jsonify({
        'interaction': interaction,
        'purchase': purchase,
        'view': view
    })

@app.route('/api/item-categories')
def item_categories():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("SELECT item_id, category FROM item_categories")
        rows = cursor.fetchall()
    conn.close()
    return jsonify({row['item_id']: row['category'] for row in rows})

@app.route('/api/category-heat')
def category_heat():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT h.item_id, h.total_hot_score, ic.category
            FROM hot_products h
            JOIN item_categories ic ON h.item_id = ic.item_id
            ORDER BY h.window_start DESC
            LIMIT 1000
        """)
        rows = cursor.fetchall()
    conn.close()

    category_sum = {}
    for r in rows:
        cat = r['category'] or '未知'
        category_sum[cat] = category_sum.get(cat, 0) + r['total_hot_score']
    
    result = [{'category': k, 'total_heat': v} for k, v in category_sum.items()]
    result.sort(key=lambda x: x['total_heat'], reverse=True)
    return jsonify(result[:20])

@app.route('/api/summary')
def summary():
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(DISTINCT item_id) as total_items FROM hot_products")
        total_items = cursor.fetchone()['total_items']
        cursor.execute("SELECT COUNT(DISTINCT category) as total_categories FROM item_categories")
        total_categories = cursor.fetchone()['total_categories']
        cursor.execute("SELECT MAX(window_end) as latest_window FROM hot_products")
        latest = cursor.fetchone()['latest_window']
    conn.close()
    return jsonify({
        'total_items': total_items,
        'total_categories': total_categories,
        'latest_window': latest.strftime('%Y-%m-%d %H:%M:%S') if latest else '暂无'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
