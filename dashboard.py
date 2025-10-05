"""
Dashboard for Tweet Analytics Pipeline
Visualizes hashtag co-occurrence network and sentiment analysis results.
"""

import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, callback
import dash_cytoscape as cyto
import logging
from datetime import datetime
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TweetAnalyticsDashboard:
    def __init__(self, out_dir="./out"):
        """
        Initialize dashboard
        
        Args:
            out_dir: Directory containing the analysis results
        """
        self.out_dir = out_dir
        self.vertices_path = os.path.join(out_dir, "vertices.json")
        self.edges_path = os.path.join(out_dir, "edges.json")
        
        # Initialize Dash app
        self.app = dash.Dash(__name__)
        self.setup_layout()
        self.setup_callbacks()
        
    def load_data(self):
        """
        Load vertices and edges data from JSON files
        
        Returns:
            Tuple of (vertices_df, edges_df)
        """
        try:
            if os.path.exists(self.vertices_path) and os.path.exists(self.edges_path):
                vertices_df = pd.read_json(self.vertices_path)
                edges_df = pd.read_json(self.edges_path)
                
                logger.info(f"Loaded {len(vertices_df)} vertices and {len(edges_df)} edges")
                return vertices_df, edges_df
            else:
                logger.warning("Data files not found. Please run the Spark processor first.")
                return pd.DataFrame(), pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
    
    def create_network_graph(self, vertices_df, edges_df, top_n=20):
        """
        Create network graph visualization
        
        Args:
            vertices_df: DataFrame with vertices data
            edges_df: DataFrame with edges data
            top_n: Number of top hashtags to display
            
        Returns:
            Cytoscape elements for network visualization
        """
        if vertices_df.empty or edges_df.empty:
            return []
        
        # Get top N hashtags by degree
        top_vertices = vertices_df.nlargest(top_n, 'degree')
        top_hashtag_ids = set(top_vertices['id'].tolist())
        
        # Filter edges to include only top hashtags
        filtered_edges = edges_df[
            (edges_df['src'].isin(top_hashtag_ids)) & 
            (edges_df['dst'].isin(top_hashtag_ids))
        ]
        
        # Create Cytoscape elements
        elements = []
        
        # Add vertices (nodes)
        for _, vertex in top_vertices.iterrows():
            sentiment_color = {
                'positive': '#4CAF50',  # Green
                'negative': '#F44336',  # Red
                'neutral': '#9E9E9E'    # Gray
            }.get(vertex['dominant_sentiment'], '#9E9E9E')
            
            elements.append({
                'data': {
                    'id': vertex['id'],
                    'label': f"#{vertex['id']}",
                    'degree': vertex['degree'],
                    'sentiment': vertex['dominant_sentiment']
                },
                'style': {
                    'background-color': sentiment_color,
                    'width': max(20, min(80, vertex['degree'] * 2)),  # Size based on degree
                    'height': max(20, min(80, vertex['degree'] * 2)),
                    'label': f"#{vertex['id']}"
                }
            })
        
        # Add edges
        for _, edge in filtered_edges.iterrows():
            elements.append({
                'data': {
                    'source': edge['src'],
                    'target': edge['dst'],
                    'weight': edge['weight']
                },
                'style': {
                    'width': max(1, min(10, edge['weight'] / 2)),  # Edge width based on weight
                    'line-color': '#666'
                }
            })
        
        return elements
    
    def create_sentiment_distribution_chart(self, vertices_df):
        """
        Create sentiment distribution chart
        
        Args:
            vertices_df: DataFrame with vertices data
            
        Returns:
            Plotly figure
        """
        if vertices_df.empty:
            return go.Figure()
        
        sentiment_counts = vertices_df['dominant_sentiment'].value_counts()
        
        fig = px.pie(
            values=sentiment_counts.values,
            names=sentiment_counts.index,
            title="Hashtag Sentiment Distribution",
            color_discrete_map={
                'positive': '#4CAF50',
                'negative': '#F44336',
                'neutral': '#9E9E9E'
            }
        )
        
        return fig
    
    def create_top_hashtags_chart(self, vertices_df, top_n=20):
        """
        Create top hashtags by degree chart
        
        Args:
            vertices_df: DataFrame with vertices data
            top_n: Number of top hashtags to show
            
        Returns:
            Plotly figure
        """
        if vertices_df.empty:
            return go.Figure()
        
        top_hashtags = vertices_df.nlargest(top_n, 'degree')
        
        # Color by sentiment
        colors = top_hashtags['dominant_sentiment'].map({
            'positive': '#4CAF50',
            'negative': '#F44336',
            'neutral': '#9E9E9E'
        })
        
        fig = go.Figure(data=[
            go.Bar(
                x=top_hashtags['degree'],
                y=[f"#{tag}" for tag in top_hashtags['id']],
                orientation='h',
                marker_color=colors,
                text=top_hashtags['dominant_sentiment'],
                textposition='inside'
            )
        ])
        
        fig.update_layout(
            title=f"Top {top_n} Hashtags by Co-occurrence Degree",
            xaxis_title="Degree (Co-occurrence Count)",
            yaxis_title="Hashtags",
            height=600
        )
        
        return fig
    
    def setup_layout(self):
        """Setup the dashboard layout"""
        self.app.layout = html.Div([
            html.Div([
                html.H1("Tweet Analytics Dashboard", 
                       style={'textAlign': 'center', 'color': '#2c3e50'}),
                html.P("Real-time visualization of hashtag co-occurrence network and sentiment analysis",
                       style={'textAlign': 'center', 'color': '#7f8c8d'})
            ], style={'padding': '20px'}),
            
            # Controls
            html.Div([
                html.Div([
                    html.Label("Top N Hashtags:", style={'fontWeight': 'bold'}),
                    dcc.Slider(
                        id='top-n-slider',
                        min=5,
                        max=50,
                        step=5,
                        value=20,
                        marks={i: str(i) for i in range(5, 51, 10)},
                        tooltip={"placement": "bottom", "always_visible": True}
                    )
                ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),
                
                html.Div([
                    html.Label("Auto-refresh (seconds):", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='refresh-interval',
                        options=[
                            {'label': 'Off', 'value': 0},
                            {'label': '30 seconds', 'value': 30},
                            {'label': '1 minute', 'value': 60},
                            {'label': '5 minutes', 'value': 300}
                        ],
                        value=60
                    )
                ], style={'width': '48%', 'float': 'right', 'display': 'inline-block', 'padding': '10px'}),
            ], style={'backgroundColor': '#ecf0f1', 'padding': '20px'}),
            
            # Data refresh interval component
            dcc.Interval(
                id='interval-component',
                interval=60*1000,  # Default 1 minute
                n_intervals=0
            ),
            
            # Last updated timestamp
            html.Div(id='last-updated', 
                    style={'textAlign': 'center', 'color': '#7f8c8d', 'padding': '10px'}),
            
            # Main content
            html.Div([
                # Network graph
                html.Div([
                    html.H3("Hashtag Co-occurrence Network", 
                           style={'textAlign': 'center', 'color': '#34495e'}),
                    cyto.Cytoscape(
                        id='network-graph',
                        layout={'name': 'circle'},
                        style={'width': '100%', 'height': '500px'},
                        elements=[],
                        stylesheet=[
                            {
                                'selector': 'node',
                                'style': {
                                    'content': 'data(label)',
                                    'text-valign': 'center',
                                    'text-halign': 'center',
                                    'font-size': '10px'
                                }
                            },
                            {
                                'selector': 'edge',
                                'style': {
                                    'curve-style': 'bezier'
                                }
                            }
                        ]
                    )
                ], style={'width': '60%', 'display': 'inline-block', 'padding': '10px'}),
                
                # Charts
                html.Div([
                    # Sentiment distribution
                    dcc.Graph(id='sentiment-chart'),
                    
                    # Top hashtags
                    dcc.Graph(id='top-hashtags-chart')
                ], style={'width': '40%', 'float': 'right', 'display': 'inline-block', 'padding': '10px'})
            ], style={'padding': '20px'})
        ])
    
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        @self.app.callback(
            [Output('network-graph', 'elements'),
             Output('sentiment-chart', 'figure'),
             Output('top-hashtags-chart', 'figure'),
             Output('last-updated', 'children')],
            [Input('interval-component', 'n_intervals'),
             Input('top-n-slider', 'value')]
        )
        def update_dashboard(n_intervals, top_n):
            # Load latest data
            vertices_df, edges_df = self.load_data()
            
            # Create visualizations
            network_elements = self.create_network_graph(vertices_df, edges_df, top_n)
            sentiment_fig = self.create_sentiment_distribution_chart(vertices_df)
            top_hashtags_fig = self.create_top_hashtags_chart(vertices_df, top_n)
            
            # Update timestamp
            last_updated = f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            return network_elements, sentiment_fig, top_hashtags_fig, last_updated
        
        @self.app.callback(
            Output('interval-component', 'interval'),
            [Input('refresh-interval', 'value')]
        )
        def update_refresh_interval(refresh_value):
            return refresh_value * 1000 if refresh_value > 0 else 999999999  # Very large number to disable
    
    def run(self, host='127.0.0.1', port=8050, debug=False):
        """
        Run the dashboard server
        
        Args:
            host: Host address
            port: Port number
            debug: Enable debug mode
        """
        logger.info(f"Starting dashboard at http://{host}:{port}")
        self.app.run_server(host=host, port=port, debug=debug)

def main():
    """Main function to run the dashboard"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Tweet Analytics Dashboard")
    parser.add_argument("--out-dir", default=os.environ.get("OUT_DIR", "./out"),
                       help="Directory containing analysis results")
    parser.add_argument("--host", default="127.0.0.1", help="Host address")
    parser.add_argument("--port", type=int, default=8050, help="Port number")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    
    dashboard = TweetAnalyticsDashboard(args.out_dir)
    dashboard.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()