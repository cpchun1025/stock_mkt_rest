from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
import pyodbc
import os
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)  # Enable CORS for cross-origin requests

# Initialize Flask-RESTful API
api = Api(app)

# Swagger UI setup
SWAGGER_URL = '/swagger'  # Swagger UI endpoint
API_URL = '/swagger.json'  # URL for Swagger specification
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "Sentiment Trends API"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Database connection settings (update these with your actual DB details)
DB_SERVER = os.getenv('DB_SERVER')  # Get DB server from environment variables
DB_NAME = os.getenv('DB_NAME')  # Get DB name from environment variables

def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"Trusted_Connection=yes;"
    )
    return conn

# Define the API resource for sentiment trends
class SentimentTrends(Resource):
    def get(self):
        """
        Get sentiment trends data
        ---
        parameters:
          - in: query
            name: stock_symbol
            type: string
            description: Filter by stock symbol
          - in: query
            name: sentiment_label
            type: string
            description: Filter by sentiment label
          - in: query
            name: start_date
            type: string
            format: date
            description: Filter by start date (YYYY-MM-DD)
          - in: query
            name: end_date
            type: string
            format: date
            description: Filter by end date (YYYY-MM-DD)
        responses:
          200:
            description: A list of sentiment trends
            schema:
              type: array
              items:
                type: object
                properties:
                  stock_symbol:
                    type: string
                    description: Stock symbol
                  published_at:
                    type: string
                    format: date
                    description: Published date
                  sentiment_label:
                    type: string
                    description: Sentiment label
                  confidence_score:
                    type: number
                    description: Confidence score
        """
        # Get query parameters for filtering
        stock_symbol = request.args.get('stock_symbol')
        sentiment_label = request.args.get('sentiment_label')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # Build the SQL query with optional WHERE conditions
        query = "SELECT stock_symbol, published_at, sentiment_label, confidence_score FROM NewsArticles WHERE 1=1"
        
        if stock_symbol:
            query += f" AND stock_symbol = '{stock_symbol}'"
        
        if sentiment_label:
            query += f" AND sentiment_label = '{sentiment_label}'"
        
        if start_date and end_date:
            query += f" AND published_at BETWEEN '{start_date}' AND '{end_date}'"

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Format the results into JSON
            results = []
            for row in rows:
                results.append({
                    'stock_symbol': row.stock_symbol,
                    'published_at': row.published_at.strftime('%Y-%m-%d'),
                    'sentiment_label': row.sentiment_label,
                    'confidence_score': row.confidence_score
                })
            return jsonify(results)
        except Exception as e:
            return jsonify({"error": str(e)})
        finally:
            cursor.close()
            conn.close()

# Add the resource to the API
api.add_resource(SentimentTrends, '/api/sentiment-trends')

# Swagger JSON specification
@app.route('/swagger.json')
def swagger_spec():
    swagger_info = {
        "swagger": "2.0",
        "info": {
            "title": "Sentiment Trends API",
            "description": "API for retrieving sentiment trends from news articles.",
            "version": "1.0.0"
        },
        "basePath": "/",
        "paths": {
            "/api/sentiment-trends": {
                "get": {
                    "summary": "Get sentiment trends",
                    "parameters": [
                        {
                            "name": "stock_symbol",
                            "in": "query",
                            "type": "string",
                            "description": "Filter by stock symbol"
                        },
                        {
                            "name": "sentiment_label",
                            "in": "query",
                            "type": "string",
                            "description": "Filter by sentiment label"
                        },
                        {
                            "name": "start_date",
                            "in": "query",
                            "type": "string",
                            "format": "date",
                            "description": "Filter by start date"
                        },
                        {
                            "name": "end_date",
                            "in": "query",
                            "type": "string",
                            "format": "date",
                            "description": "Filter by end date"
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "A list of sentiment trends",
                            "schema": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "stock_symbol": {
                                            "type": "string"
                                        },
                                        "published_at": {
                                            "type": "string",
                                            "format": "date"
                                        },
                                        "sentiment_label": {
                                            "type": "string"
                                        },
                                        "confidence_score": {
                                            "type": "number"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return jsonify(swagger_info)

if __name__ == '__main__':
    app.run(debug=True)