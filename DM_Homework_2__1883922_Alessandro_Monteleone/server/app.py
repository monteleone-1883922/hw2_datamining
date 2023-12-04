import pandas as pd
from flask import Flask, render_template, request
from markupsafe import Markup
from hashing_techniques import load_report
from utils_for_hashing import PATH_REPORT,PATH_REPORT_SPARK
from amazon_product_analysis import *
from utils_and_classes import CATEGORIES, STOPWORDS_FILE_PATH, SPECIAL_CHARACTERS_FILE_PATH, MAX_NUM_RESULTS_FOR_QUERY

app = Flask(__name__)
df = load_and_retype_data()
qp = QueryProcessor(INDEX_FILE_PATH, STOPWORDS_FILE_PATH,
                    SPECIAL_CHARACTERS_FILE_PATH)
report = None
report_spark = None

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/query_menu')
def query_menu():
    table = df.to_html()
    table = Markup(table)
    return render_template('base.html', table=table, query="")


@app.route('/top10price', methods=['get'])
def top10_prices():
    table = topN(df.copy(), "price", 10).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table, query="Top 10 products by price")


@app.route('/top10rated', methods=['get'])
def top10_reviews():
    table = topN(df.copy(), "stars", 10).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table, query="Top 10 products by ratings")


@app.route('/top10rated_weighted', methods=['get'])
def top10_reviews_weighted():
    local_df = df.copy()
    compute_weighted_ratings(local_df)
    table = topN(local_df, "weighted_ratings", 10).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table, query="Top 10 products by weighted ratings")


@app.route('/price_categories', methods=['get'])
def price_categories():
    table = price_range_for_categories(df.copy()[["description", "price"]], CATEGORIES).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table, query="Price range for different series of gpu")


@app.route('/fixed_queries', methods=['get'])
def fixed_queries():
    return render_template('fixed_queries_menu.html')


@app.route('/query', methods=['post'])
def process_query():
    user_input = request.form['user_input']
    heap = qp.query_process(user_input)
    table = get_query_result(df.copy(), heap, MAX_NUM_RESULTS_FOR_QUERY).to_html()
    table = Markup(table)
    return render_template('base.html', table=table, query=user_input)

@app.route('/analyze_primeness', methods=['get'])
def analyze_primeness_query():
    table = analyze_primeness(df.copy()).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table, query="Analyze primeness")

@app.route('/nearest_products', methods=['get'])
def nearest_products_reports():
    report = load_report(PATH_REPORT)
    # lsh_result = {tuple(el) for el in report["lsh_result"]}
    # jaccard_result = {tuple(el) for el in report["jaccard_result"]}
    # print("false positives = ", len(lsh_result.difference(jaccard_result)))
    # print("false negatives = ", len(jaccard_result.difference(lsh_result)))
    report_spark = load_report(PATH_REPORT_SPARK)
    return render_template('report_comparations.html',
                           jaccard_num_duplicates=report["jaccard_num_duplicates"],
                           lsh_num_duplicates=report["lsh_num_duplicates"],
                           size_intersection_results=report["size_intersection_results"],
                           lsh_last_execution_time=report["lsh_last_execution_time"],
                           jaccard_last_execution_time=report["jaccard_last_execution_time"],
                           jaccard_num_duplicates_spark=report_spark["jaccard_num_duplicates"],
                           lsh_num_duplicates_spark=report_spark["lsh_num_duplicates"],
                           size_intersection_results_spark=report_spark["size_intersection_results"],
                           lsh_last_execution_time_spark=report_spark["lsh_last_execution_time"],
                           jaccard_last_execution_time_spark=report_spark["jaccard_last_execution_time"]
                           )

@app.route('/lsh_nearest', methods=['get'])
def nearest_lsh():
    report = load_report(PATH_REPORT)
    lsh_result = report["lsh_result"]
    doc1_id = [el[0] for el in lsh_result]
    doc2_id = [el[1] for el in lsh_result]
    descriptions = df["description"]
    doc1 = descriptions.iloc[doc1_id].tolist()
    doc2 = descriptions.iloc[doc2_id].tolist()
    data = pd.DataFrame({"Description 1": doc1,"Id product 1": doc1_id, "Description 2": doc2, "Id product 2": doc2_id})
    data = data.to_html()
    table = Markup(data)
    return render_template('nearest_documents.html', table=table,algorithm="LSH")

@app.route('/jaccard_nearest', methods=['get'])
def nearest_jaccard():
    report = load_report(PATH_REPORT)
    jaccard_result = report["jaccard_result"]
    doc1_id = [el[0] for el in jaccard_result]
    doc2_id = [el[1] for el in jaccard_result]
    descriptions = df["description"]
    doc1 = descriptions.iloc[doc1_id].tolist()
    doc2 = descriptions.iloc[doc2_id].tolist()
    data = pd.DataFrame({"Description 1": doc1,"Id product 1": doc1_id, "Description 2": doc2, "Id product 2": doc2_id})
    data = data.to_html()
    table = Markup(data)
    return render_template('nearest_documents.html', table=table,algorithm="JACCARD SIMILARITY")




if __name__ == '__main__':
    app.run(debug=True)
