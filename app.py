import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql
import plotly.graph_objects as go
import plotly.express as px
import warnings
import os
from io import BytesIO

warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Business Development Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #374151;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #FFFFFF;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    .period-selector {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

class PerformanceReportGenerator:
    def __init__(self):
        # Define dynamic date ranges
        self.today = datetime.now()
        
        # Base period: Oct 1, 2025 to Jan 14, 2026
        self.start_date_overall = datetime(2025, 10, 1)
        
        # Set end date as Jan 14, 2026 or today if earlier
        self.end_date_overall = min(datetime(2026, 1, 14), self.today)
        
        # Define all products and their categories
        self.product_categories = {
            'P2P (Internal Wallet Transfer)': ['Internal Wallet Transfer'],
            'Cash-In': ['Deposit'],
            'Cash-Out': ['Scan To Withdraw Agent', 'Scan To Withdraw Customer', 'OTP Withdrawal'],
            'Disbursement': ['Disbursement'],
            'Cash Power': ['Nawec Cashpower'],
            'E-Ticketing': ['Ticket'],
            'Bank Transfers': ['BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER']
        }
        
        # Airtime Topup is a Service, not Product
        self.services = ['Airtime Topup']
        
        # Flatten product list for filtering
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
        
        # Add service to all products list for reporting
        self.all_products.append('Airtime Topup')
        
        # Track product performance history for consistency analysis
        self.product_performance_history = {}
        
        # Database connection
        self.db_connection = None
    
    def connect_to_database(self):
        """Establish connection to MySQL database"""
        try:
            self.db_connection = pymysql.connect(
                host='db4free.net',
                port=3306,  # Default MySQL port
                user='lamin_d_kinteh',
                password='Lamin@123',
                database='bdp_report',  # Add your database name
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return True
        except Exception as e:
            st.error(f"Failed to connect to database: {e}")
            return False
    
    def load_transaction_data_from_db(self, start_date, end_date):
        """Load transaction data from MySQL database"""
        query = """
        SELECT 
            user_identifier,
            transaction_id,
            sub_transaction_id,
            entity_name,
            status,
            service_name,
            product_name,
            transaction_type,
            amount,
            ucp_name,
            reference,
            remarks,
            created_at
        FROM transactions
        WHERE created_at BETWEEN %s AND %s
        ORDER BY created_at DESC
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(query, (start_date, end_date))
                results = cursor.fetchall()
                df = pd.DataFrame(results)
                
                # Convert date column
                df['created_at'] = pd.to_datetime(df['created_at'])
                
                return df
        except Exception as e:
            st.error(f"Error loading transaction data: {e}")
            return pd.DataFrame()
    
    def load_onboarding_data_from_db(self, start_date, end_date):
        """Load onboarding data from MySQL database"""
        query = """
        SELECT 
            account_id,
            full_name,
            mobile,
            email,
            kyc_status,
            registration_date,
            updated_at,
            entity,
            status
        FROM onboarding
        WHERE registration_date BETWEEN %s AND %s
        ORDER BY registration_date DESC
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(query, (start_date, end_date))
                results = cursor.fetchall()
                df = pd.DataFrame(results)
                
                # Convert date columns
                df['registration_date'] = pd.to_datetime(df['registration_date'])
                df['updated_at'] = pd.to_datetime(df['updated_at'])
                
                # Create user identifier from mobile
                df['user_identifier'] = df['mobile'].astype(str).str.strip()
                
                return df
        except Exception as e:
            st.error(f"Error loading onboarding data: {e}")
            return pd.DataFrame()
    
    def load_data(self, start_date=None, end_date=None):
        """Load data from MySQL database"""
        if start_date is None:
            start_date = self.start_date_overall
        if end_date is None:
            end_date = self.end_date_overall
        
        with st.spinner("Loading data from database..."):
            # Connect to database
            if not self.connect_to_database():
                return False
            
            # Load transaction data
            self.transactions = self.load_transaction_data_from_db(start_date, end_date)
            
            if self.transactions.empty:
                st.warning("No transaction data found for the selected period")
                return False
            
            # Load onboarding data
            self.onboarding = self.load_onboarding_data_from_db(start_date, end_date)
            
            if self.onboarding.empty:
                st.warning("No onboarding data found for the selected period")
                return False
            
            # Data quality summary
            st.success(f"✅ Loaded {len(self.transactions):,} transaction records")
            st.success(f"✅ Loaded {len(self.onboarding):,} onboarding records")
            
            return True
    
    def create_date_filters(self):
        """Create flexible date filters in sidebar"""
        st.sidebar.header("📅 Date Filters")
        
        # Date range selector
        min_date = datetime(2025, 10, 1)
        max_date = datetime.now()
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=self.start_date_overall,
                min_value=min_date,
                max_value=max_date
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                value=self.end_date_overall,
                min_value=min_date,
                max_value=max_date
            )
        
        # Convert to datetime
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.max.time())
        
        # Quick select buttons
        st.sidebar.subheader("Quick Select")
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("Last 7 Days"):
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
        with col2:
            if st.button("Last 30 Days"):
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
        
        col3, col4 = st.sidebar.columns(2)
        with col3:
            if st.button("This Month"):
                today = datetime.now()
                start_date = datetime(today.year, today.month, 1)
                end_date = today
        with col4:
            if st.button("Last Month"):
                today = datetime.now()
                if today.month == 1:
                    start_date = datetime(today.year - 1, 12, 1)
                    end_date = datetime(today.year - 1, 12, 31)
                else:
                    start_date = datetime(today.year, today.month - 1, 1)
                    end_date = datetime(today.year, today.month - 1, 1) + timedelta(days=31)
                    end_date = datetime(end_date.year, end_date.month, 1) - timedelta(days=1)
        
        return start_date, end_date
    
    def create_period_type_filter(self):
        """Create period type filter"""
        period_type = st.sidebar.selectbox(
            "Period Type",
            ["Weekly", "Monthly", "7-Day Rolling", "Custom"],
            help="Select how to aggregate the data"
        )
        return period_type.lower().replace("-day ", "")
    
    def create_product_filter(self):
        """Create product filter"""
        st.sidebar.header("📊 Product Filters")
        
        # Multi-select for products
        selected_products = st.sidebar.multiselect(
            "Select Products",
            options=self.all_products,
            default=self.all_products,
            help="Select products to include in analysis"
        )
        
        # Category filter
        categories = list(self.product_categories.keys()) + ['Airtime Topup']
        selected_categories = st.sidebar.multiselect(
            "Select Categories",
            options=categories,
            default=categories,
            help="Select product categories to include"
        )
        
        # Get products from selected categories
        category_products = []
        for category in selected_categories:
            if category == 'Airtime Topup':
                category_products.append('Airtime Topup')
            elif category in self.product_categories:
                category_products.extend(self.product_categories[category])
        
        # Combine both filters
        filtered_products = list(set(selected_products) & set(category_products))
        
        return filtered_products if filtered_products else self.all_products
    
    def get_periods_for_dates(self, start_date, end_date, period_type):
        """Generate periods based on selected date range and period type"""
        periods = []
        
        if period_type == 'weekly':
            # Generate weekly periods
            current_start = start_date
            while current_start <= end_date:
                current_end = min(current_start + timedelta(days=6), end_date)
                period_name = f"Week {current_start.strftime('%U')}: {current_start.strftime('%b %d')} - {current_end.strftime('%b %d')}"
                periods.append((period_name, current_start, current_end, 'weekly'))
                current_start = current_end + timedelta(days=1)
                
        elif period_type == 'monthly':
            # Generate monthly periods
            current_start = start_date.replace(day=1)
            while current_start <= end_date:
                if current_start.month == 12:
                    next_month = current_start.replace(year=current_start.year + 1, month=1, day=1)
                else:
                    next_month = current_start.replace(month=current_start.month + 1, day=1)
                
                current_end = min(next_month - timedelta(days=1), end_date)
                period_name = f"{current_start.strftime('%B %Y')}"
                periods.append((period_name, current_start, current_end, 'monthly'))
                current_start = next_month
                
        elif period_type == 'rolling':
            # Generate 7-day rolling periods
            current_start = start_date
            while current_start <= end_date:
                current_end = min(current_start + timedelta(days=6), end_date)
                if (current_end - current_start).days >= 6:  # Only include full weeks
                    period_name = f"Rolling: {current_start.strftime('%b %d')} - {current_end.strftime('%b %d')}"
                    periods.append((period_name, current_start, current_end, 'rolling'))
                current_start = current_start + timedelta(days=7)
        
        else:  # Custom
            period_name = f"Custom: {start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}"
            periods.append((period_name, start_date, end_date, 'custom'))
        
        return periods
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range"""
        if date_col not in df.columns or df.empty:
            return pd.DataFrame()
        
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date)
        return df[mask].copy()
    
    def calculate_executive_metrics(self, start_date, end_date, period_type):
        """Calculate executive metrics for the period"""
        metrics = {}
        
        # Filter data for period
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        period_onboarding = self.filter_by_date_range(
            self.onboarding, 'registration_date', start_date, end_date
        )
        
        # 1. New Customers
        if not period_onboarding.empty:
            new_customers = period_onboarding[
                (period_onboarding['entity'] == 'Customer') &
                (period_onboarding['status'].isin(['Active', 'Registered', 'TemporaryRegister']))
            ]['user_identifier'].nunique()
        else:
            new_customers = 0
        
        metrics['new_customers'] = new_customers
        
        # 2. Active Customers
        if not period_transactions.empty:
            customer_transactions = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Count transactions per user
                user_counts = customer_transactions.groupby('user_identifier').size()
                
                # Set threshold based on period type
                threshold = 2 if period_type in ['weekly', 'rolling'] else 10
                active_customers = (user_counts >= threshold).sum()
            else:
                active_customers = 0
        else:
            active_customers = 0
        
        metrics['active_customers'] = active_customers
        
        # 3. Total Transaction Value
        if not period_transactions.empty:
            successful_transactions = period_transactions[
                period_transactions['status'] == 'SUCCESS'
            ]
            total_value = successful_transactions['amount'].sum()
        else:
            total_value = 0
        
        metrics['total_transaction_value'] = total_value
        
        # 4. Transaction Count
        if not period_transactions.empty:
            transaction_count = len(period_transactions[
                period_transactions['status'] == 'SUCCESS'
            ])
        else:
            transaction_count = 0
        
        metrics['transaction_count'] = transaction_count
        
        # 5. Top Product
        if not period_transactions.empty:
            # Filter customer transactions
            customer_tx = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS')
            ]
            
            # Special handling for P2P
            p2p_tx = customer_tx[
                (customer_tx['product_name'] == 'Internal Wallet Transfer') &
                (customer_tx['transaction_type'] == 'DR')
            ]
            
            # Exclude fees
            if 'ucp_name' in p2p_tx.columns:
                p2p_tx = p2p_tx[~p2p_tx['ucp_name'].str.contains('Fee', na=False)]
            
            # Count by product
            product_counts = {}
            for product in self.all_products:
                if product == 'Internal Wallet Transfer':
                    product_counts[product] = len(p2p_tx)
                elif product == 'Airtime Topup':
                    airtime_tx = customer_tx[
                        (customer_tx['service_name'] == 'Airtime Topup') &
                        (customer_tx['transaction_type'] == 'DR')
                    ]
                    product_counts[product] = len(airtime_tx)
                else:
                    product_tx = customer_tx[customer_tx['product_name'] == product]
                    product_counts[product] = len(product_tx)
            
            if product_counts:
                top_product = max(product_counts, key=product_counts.get)
                top_product_count = product_counts[top_product]
                
                # Get lowest performing product with at least 1 transaction
                active_products = {k: v for k, v in product_counts.items() if v > 0}
                if active_products:
                    low_product = min(active_products, key=active_products.get)
                    low_product_count = active_products[low_product]
                else:
                    low_product = 'N/A'
                    low_product_count = 0
                
                metrics.update({
                    'top_product': top_product,
                    'top_product_count': top_product_count,
                    'low_product': low_product,
                    'low_product_count': low_product_count
                })
        
        return metrics
    
    def calculate_product_metrics(self, start_date, end_date, filtered_products):
        """Calculate metrics for each product"""
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        product_metrics = {}
        
        for product in filtered_products:
            if product == 'Internal Wallet Transfer':
                product_tx = period_transactions[
                    (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                    (period_transactions['entity_name'] == 'Customer') &
                    (period_transactions['status'] == 'SUCCESS') &
                    (period_transactions['transaction_type'] == 'DR')
                ]
                
                if 'ucp_name' in product_tx.columns:
                    product_tx = product_tx[~product_tx['ucp_name'].str.contains('Fee', na=False)]
                    
            elif product == 'Airtime Topup':
                product_tx = period_transactions[
                    (period_transactions['service_name'] == 'Airtime Topup') &
                    (period_transactions['entity_name'] == 'Customer') &
                    (period_transactions['status'] == 'SUCCESS') &
                    (period_transactions['transaction_type'] == 'DR')
                ]
            else:
                product_tx = period_transactions[
                    (period_transactions['product_name'] == product) &
                    (period_transactions['entity_name'] == 'Customer') &
                    (period_transactions['status'] == 'SUCCESS')
                ]
            
            if not product_tx.empty:
                total_transactions = len(product_tx)
                total_amount = product_tx['amount'].sum()
                unique_users = product_tx['user_identifier'].nunique()
                avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                
                product_metrics[product] = {
                    'transactions': total_transactions,
                    'amount': total_amount,
                    'users': unique_users,
                    'avg_amount': avg_amount
                }
            else:
                product_metrics[product] = {
                    'transactions': 0,
                    'amount': 0,
                    'users': 0,
                    'avg_amount': 0
                }
        
        return product_metrics
    
    def create_executive_summary_dashboard(self, metrics, start_date, end_date):
        """Create executive summary dashboard"""
        st.markdown("<h2 class='sub-header'>📈 Executive Summary</h2>", unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="New Customers",
                value=f"{metrics.get('new_customers', 0):,}",
                delta=None
            )
        
        with col2:
            st.metric(
                label="Active Customers",
                value=f"{metrics.get('active_customers', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="Total Transactions",
                value=f"{metrics.get('transaction_count', 0):,}",
                delta=None
            )
        
        with col4:
            st.metric(
                label="Total Value",
                value=f"GMD {metrics.get('total_transaction_value', 0):,.2f}",
                delta=None
            )
        
        # Top and Bottom Products
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.subheader("🏆 Top Performing Product")
            if 'top_product' in metrics and metrics['top_product'] != 'N/A':
                st.write(f"**{metrics['top_product']}**")
                st.write(f"Transactions: {metrics.get('top_product_count', 0):,}")
            else:
                st.write("No data available")
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.subheader("📉 Lowest Performing Product")
            if 'low_product' in metrics and metrics['low_product'] != 'N/A':
                st.write(f"**{metrics['low_product']}**")
                st.write(f"Transactions: {metrics.get('low_product_count', 0):,}")
            else:
                st.write("No data available")
            st.markdown("</div>", unsafe_allow_html=True)
    
    def create_product_performance_chart(self, product_metrics):
        """Create product performance chart"""
        if not product_metrics:
            st.warning("No product data available")
            return
        
        # Prepare data for chart
        products = list(product_metrics.keys())
        transactions = [product_metrics[p]['transactions'] for p in products]
        amounts = [product_metrics[p]['amount'] for p in products]
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Transaction Count", "Transaction Value", "User Count"])
        
        with tab1:
            fig = px.bar(
                x=products,
                y=transactions,
                title="Product Performance by Transaction Count",
                labels={'x': 'Product', 'y': 'Number of Transactions'},
                color=transactions,
                color_continuous_scale='Viridis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            fig = px.bar(
                x=products,
                y=amounts,
                title="Product Performance by Transaction Value",
                labels={'x': 'Product', 'y': 'Total Amount (GMD)'},
                color=amounts,
                color_continuous_scale='Plasma'
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            users = [product_metrics[p]['users'] for p in products]
            fig = px.bar(
                x=products,
                y=users,
                title="Product Performance by Unique Users",
                labels={'x': 'Product', 'y': 'Number of Unique Users'},
                color=users,
                color_continuous_scale='Cividis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    def create_trend_analysis(self, periods_data):
        """Create trend analysis across periods"""
        if len(periods_data) < 2:
            st.warning("Need at least 2 periods for trend analysis")
            return
        
        # Prepare trend data
        trend_df = pd.DataFrame([
            {
                'Period': name,
                'New Customers': metrics.get('new_customers', 0),
                'Active Customers': metrics.get('active_customers', 0),
                'Transactions': metrics.get('transaction_count', 0),
                'Total Value': metrics.get('total_transaction_value', 0)
            }
            for name, _, _, metrics in periods_data
        ])
        
        # Create trend chart
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=trend_df['Period'],
            y=trend_df['New Customers'],
            mode='lines+markers',
            name='New Customers',
            line=dict(color='#1E3A8A', width=3)
        ))
        
        fig.add_trace(go.Scatter(
            x=trend_df['Period'],
            y=trend_df['Active Customers'],
            mode='lines+markers',
            name='Active Customers',
            line=dict(color='#10B981', width=3)
        ))
        
        fig.add_trace(go.Scatter(
            x=trend_df['Period'],
            y=trend_df['Transactions'],
            mode='lines+markers',
            name='Transactions',
            line=dict(color='#8B5CF6', width=3),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Performance Trends Over Time',
            xaxis=dict(title='Period'),
            yaxis=dict(title='Customer Count'),
            yaxis2=dict(
                title='Transaction Count',
                overlaying='y',
                side='right'
            ),
            legend=dict(x=0, y=1.1, orientation='h'),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def create_product_trend_chart(self, periods_data):
        """Create product trend chart across periods"""
        if len(periods_data) < 2:
            return
        
        # Collect product data across periods
        product_trends = {}
        
        for period_name, _, _, metrics in periods_data:
            product_metrics = metrics.get('product_metrics', {})
            for product, data in product_metrics.items():
                if product not in product_trends:
                    product_trends[product] = []
                product_trends[product].append({
                    'period': period_name,
                    'transactions': data['transactions']
                })
        
        # Create chart for top 5 products
        top_products = sorted(
            [(p, sum([d['transactions'] for d in data])) 
             for p, data in product_trends.items()],
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        if not top_products:
            return
        
        fig = go.Figure()
        
        colors = px.colors.qualitative.Set2
        for idx, (product, _) in enumerate(top_products):
            product_data = product_trends[product]
            periods = [d['period'] for d in product_data]
            transactions = [d['transactions'] for d in product_data]
            
            fig.add_trace(go.Scatter(
                x=periods,
                y=transactions,
                mode='lines+markers',
                name=product,
                line=dict(color=colors[idx % len(colors)], width=2)
            ))
        
        fig.update_layout(
            title='Top 5 Products - Transaction Trends',
            xaxis=dict(title='Period'),
            yaxis=dict(title='Transaction Count'),
            legend=dict(x=0, y=1.1, orientation='h'),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def create_detailed_data_view(self, start_date, end_date):
        """Create detailed data view"""
        st.markdown("<h2 class='sub-header'>📋 Detailed Data</h2>", unsafe_allow_html=True)
        
        tab1, tab2 = st.tabs(["Transaction Data", "Onboarding Data"])
        
        with tab1:
            # Filter transactions for period
            period_transactions = self.filter_by_date_range(
                self.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty:
                st.dataframe(
                    period_transactions,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Download button
                csv = period_transactions.to_csv(index=False)
                st.download_button(
                    label="📥 Download Transaction Data",
                    data=csv,
                    file_name=f"transactions_{start_date.date()}_to_{end_date.date()}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No transaction data available for the selected period")
        
        with tab2:
            # Filter onboarding for period
            period_onboarding = self.filter_by_date_range(
                self.onboarding, 'registration_date', start_date, end_date
            )
            
            if not period_onboarding.empty:
                st.dataframe(
                    period_onboarding,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Download button
                csv = period_onboarding.to_csv(index=False)
                st.download_button(
                    label="📥 Download Onboarding Data",
                    data=csv,
                    file_name=f"onboarding_{start_date.date()}_to_{end_date.date()}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No onboarding data available for the selected period")
    
    def create_report_download(self, periods_data):
        """Create downloadable report"""
        st.markdown("<h2 class='sub-header'>📤 Export Report</h2>", unsafe_allow_html=True)
        
        # Create summary report
        report_data = []
        for period_name, start_date, end_date, metrics in periods_data:
            report_data.append({
                'Period': period_name,
                'Start Date': start_date.strftime('%Y-%m-%d'),
                'End Date': end_date.strftime('%Y-%m-%d'),
                'New Customers': metrics.get('new_customers', 0),
                'Active Customers': metrics.get('active_customers', 0),
                'Total Transactions': metrics.get('transaction_count', 0),
                'Total Value': metrics.get('total_transaction_value', 0),
                'Top Product': metrics.get('top_product', 'N/A'),
                'Top Product Count': metrics.get('top_product_count', 0),
                'Low Product': metrics.get('low_product', 'N/A'),
                'Low Product Count': metrics.get('low_product_count', 0)
            })
        
        report_df = pd.DataFrame(report_data)
        
        # Create Excel file
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Summary sheet
            report_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Product performance sheet
            product_data = []
            for period_name, start_date, end_date, metrics in periods_data:
                product_metrics = metrics.get('product_metrics', {})
                for product, data in product_metrics.items():
                    product_data.append({
                        'Period': period_name,
                        'Product': product,
                        'Transactions': data['transactions'],
                        'Total Amount': data['amount'],
                        'Unique Users': data['users'],
                        'Average Amount': data['avg_amount']
                    })
            
            if product_data:
                product_df = pd.DataFrame(product_data)
                product_df.to_excel(writer, sheet_name='Product Performance', index=False)
        
        # Download button
        st.download_button(
            label="📊 Download Full Report (Excel)",
            data=output.getvalue(),
            file_name=f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown("<h1 class='main-header'>📊 Business Development Performance Dashboard</h1>", unsafe_allow_html=True)
    
    # Initialize generator
    generator = PerformanceReportGenerator()
    
    # Sidebar filters
    st.sidebar.header("🔧 Configuration")
    
    # Date filters
    start_date, end_date = generator.create_date_filters()
    
    # Period type filter
    period_type = generator.create_period_type_filter()
    
    # Product filters
    filtered_products = generator.create_product_filter()
    
    # Load data button
    if st.sidebar.button("🚀 Load Data", type="primary"):
        with st.spinner("Loading data..."):
            if generator.load_data(start_date, end_date):
                st.success("Data loaded successfully!")
            else:
                st.error("Failed to load data. Please check database connection.")
    
    # Check if data is loaded
    if not hasattr(generator, 'transactions') or generator.transactions.empty:
        st.info("👈 Please configure filters and click 'Load Data' to begin analysis")
        return
    
    # Generate periods
    periods = generator.get_periods_for_dates(start_date, end_date, period_type)
    
    if not periods:
        st.warning("No periods found for the selected date range and period type")
        return
    
    # Calculate metrics for each period
    periods_data = []
    for period_name, period_start, period_end, p_type in periods:
        with st.spinner(f"Analyzing {period_name}..."):
            # Calculate executive metrics
            exec_metrics = generator.calculate_executive_metrics(
                period_start, period_end, p_type
            )
            
            # Calculate product metrics
            product_metrics = generator.calculate_product_metrics(
                period_start, period_end, filtered_products
            )
            
            exec_metrics['product_metrics'] = product_metrics
            
            periods_data.append((period_name, period_start, period_end, exec_metrics))
    
    # Display results
    st.markdown(f"### 📅 Analysis Period: {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}")
    st.markdown(f"**Period Type:** {period_type.title()} | **Products:** {len(filtered_products)} selected")
    
    # Period selector
    if len(periods_data) > 1:
        selected_period = st.selectbox(
            "Select Period to View Details",
            options=[p[0] for p in periods_data],
            index=len(periods_data) - 1  # Default to latest
        )
        
        # Get selected period data
        for period_name, period_start, period_end, metrics in periods_data:
            if period_name == selected_period:
                current_period_data = (period_name, period_start, period_end, metrics)
                break
    else:
        current_period_data = periods_data[0]
        selected_period = current_period_data[0]
    
    # Executive Summary for selected period
    generator.create_executive_summary_dashboard(
        current_period_data[3],
        current_period_data[1],
        current_period_data[2]
    )
    
    # Product Performance
    st.markdown("<h2 class='sub-header'>📊 Product Performance</h2>", unsafe_allow_html=True)
    generator.create_product_performance_chart(
        current_period_data[3].get('product_metrics', {})
    )
    
    # Trend Analysis (if multiple periods)
    if len(periods_data) > 1:
        st.markdown("<h2 class='sub-header'>📈 Trend Analysis</h2>", unsafe_allow_html=True)
        
        # Performance trends
        generator.create_trend_analysis(periods_data)
        
        # Product trends
        generator.create_product_trend_chart(periods_data)
    
    # Detailed Data View
    generator.create_detailed_data_view(start_date, end_date)
    
    # Export Report
    generator.create_report_download(periods_data)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #6B7280;'>
            <p>Business Development Performance Dashboard • Data up to: {}</p>
        </div>
        """.format(end_date.strftime('%B %d, %Y')),
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
