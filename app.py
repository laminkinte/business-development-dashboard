import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
import plotly.express as px
import pymysql
from pymysql import MySQLError
import warnings
from dateutil.relativedelta import relativedelta

warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Business Development Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        font-weight: 700;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #374151;
        font-weight: 600;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #1E3A8A;
        margin-bottom: 1rem;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1E3A8A;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #6B7280;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .trend-up {
        color: #10B981;
        font-weight: 600;
    }
    .trend-down {
        color: #EF4444;
        font-weight: 600;
    }
    .trend-neutral {
        color: #6B7280;
        font-weight: 600;
    }
    .section-divider {
        border-top: 2px solid #E5E7EB;
        margin: 2rem 0;
    }
    .product-card {
        background-color: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        border: 1px solid #E5E7EB;
        transition: transform 0.2s;
    }
    .product-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }
    .warning-box {
        background-color: #FEF3C7;
        border-left: 4px solid #F59E0B;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #D1FAE5;
        border-left: 4px solid #10B981;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #DBEAFE;
        border-left: 4px solid #3B82F6;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

class PerformanceDashboard:
    def __init__(self):
        # Initialize database connection
        self.db_config = {
            'host': 'db4free.net',
            'database': 'bdp_report',
            'user': 'lamin_d_kinteh',
            'password': 'Lamin@123'
        }
        
        # Define product categories
        self.product_categories = {
            'P2P (Internal Wallet Transfer)': ['Internal Wallet Transfer'],
            'Cash-In': ['Deposit'],
            'Cash-Out': ['Scan To Withdraw Agent', 'Scan To Withdraw Customer', 'OTP Withdrawal'],
            'Disbursement': ['Disbursement'],
            'Cash Power': ['Nawec Cashpower'],
            'E-Ticketing': ['Ticket'],
            'Bank Transfers': ['BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER']
        }
        
        self.services = ['Airtime Topup']
        
        # Flatten product list
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
        self.all_products.append('Airtime Topup')
        
        # Initialize session state
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False
        if 'transactions' not in st.session_state:
            st.session_state.transactions = pd.DataFrame()
        if 'onboarding' not in st.session_state:
            st.session_state.onboarding = pd.DataFrame()
        if 'start_date' not in st.session_state:
            st.session_state.start_date = None
        if 'end_date' not in st.session_state:
            st.session_state.end_date = None
    
    def get_db_connection(self):
        """Establish MySQL database connection"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            return connection
        except MySQLError as e:
            st.error(f"Database connection failed: {e}")
            st.info("Please check your database credentials and connection")
            return None
    
    def test_db_connection(self):
        """Test database connection"""
        connection = self.get_db_connection()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    connection.close()
                    return True
            except Exception as e:
                st.error(f"Database test failed: {e}")
                return False
        return False
    
    def load_data_from_db(self, start_date, end_date):
        """Load data from MySQL database"""
        st.session_state.start_date = start_date
        st.session_state.end_date = end_date
        
        connection = self.get_db_connection()
        if not connection:
            return False
        
        try:
            with connection.cursor() as cursor:
                # Load transactions
                st.info("Loading transaction data...")
                
                transaction_query = """
                    SELECT 
                        id, user_identifier, transaction_id, sub_transaction_id,
                        entity_name, full_name, created_by, status, internal_status,
                        service_name, product_name, transaction_type, amount,
                        before_balance, after_balance, ucp_name, wallet_name,
                        pouch_name, reference, error_code, error_message,
                        vendor_transaction_id, vendor_response_code, vendor_message,
                        slug, remarks, created_at, business_hierarchy,
                        parent_user_identifier, parent_full_name
                    FROM Transaction
                    WHERE created_at BETWEEN %s AND %s
                    ORDER BY created_at
                """
                
                cursor.execute(transaction_query, (start_date, end_date))
                transactions = cursor.fetchall()
                
                # Convert to DataFrame
                if transactions:
                    transactions_df = pd.DataFrame(transactions)
                    
                    # Parse dates
                    if 'created_at' in transactions_df.columns:
                        transactions_df['created_at'] = pd.to_datetime(transactions_df['created_at'], errors='coerce')
                    
                    # Clean numeric columns
                    if 'amount' in transactions_df.columns:
                        transactions_df['amount'] = pd.to_numeric(transactions_df['amount'], errors='coerce')
                    
                    # Clean text columns
                    text_cols = ['user_identifier', 'product_name', 'entity_name', 'transaction_type', 
                               'ucp_name', 'service_name', 'status']
                    for col in text_cols:
                        if col in transactions_df.columns:
                            transactions_df[col] = transactions_df[col].astype(str).str.strip()
                    
                    st.session_state.transactions = transactions_df
                    st.success(f"✅ Loaded {len(transactions_df)} transaction records")
                else:
                    st.warning("⚠️ No transaction records found in the selected date range")
                    st.session_state.transactions = pd.DataFrame()
                
                # Load onboarding data
                st.info("Loading onboarding data...")
                
                onboarding_query = """
                    SELECT 
                        account_id, full_name, mobile, email, region, district,
                        town_village, business_name, kyc_status, registration_date,
                        updated_at, proof_of_id, identification_number,
                        customer_referrer_code, customer_referrer_mobile,
                        referrer_entity, entity, bank, bank_account_name,
                        bank_account_number, status
                    FROM Onboarding
                    WHERE registration_date BETWEEN %s AND %s
                    ORDER BY registration_date
                """
                
                cursor.execute(onboarding_query, (start_date, end_date))
                onboarding = cursor.fetchall()
                
                if onboarding:
                    onboarding_df = pd.DataFrame(onboarding)
                    
                    # Parse dates
                    if 'registration_date' in onboarding_df.columns:
                        onboarding_df['registration_date'] = pd.to_datetime(onboarding_df['registration_date'], errors='coerce')
                    
                    if 'updated_at' in onboarding_df.columns:
                        onboarding_df['updated_at'] = pd.to_datetime(onboarding_df['updated_at'], errors='coerce')
                    
                    # Create User Identifier for merging
                    if 'mobile' in onboarding_df.columns:
                        onboarding_df['user_identifier'] = onboarding_df['mobile'].astype(str).str.strip()
                    
                    st.session_state.onboarding = onboarding_df
                    st.success(f"✅ Loaded {len(onboarding_df)} onboarding records")
                else:
                    st.warning("⚠️ No onboarding records found in the selected date range")
                    st.session_state.onboarding = pd.DataFrame()
            
            connection.close()
            st.session_state.data_loaded = True
            return True
            
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            import traceback
            st.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def create_date_filters(self):
        """Create flexible date range filters"""
        st.sidebar.markdown("### 📅 Date Range Selection")
        
        # Date range options
        date_options = {
            "Last 7 Days": 7,
            "Last 30 Days": 30,
            "Last 90 Days": 90,
            "This Month": "month",
            "Last Month": "last_month",
            "This Quarter": "quarter",
            "Last Quarter": "last_quarter",
            "Year to Date": "ytd",
            "Last Year": "last_year",
            "Custom Range": "custom"
        }
        
        selected_option = st.sidebar.selectbox(
            "Select Date Range",
            list(date_options.keys()),
            index=2  # Default to Last 90 Days
        )
        
        today = datetime.now()
        
        if selected_option == "Custom Range":
            col1, col2 = st.sidebar.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=today - timedelta(days=90),
                    max_value=today
                )
            with col2:
                end_date = st.date_input(
                    "End Date",
                    value=today,
                    min_value=start_date,
                    max_value=today
                )
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
        else:
            days = date_options[selected_option]
            
            if days == "month":
                # This month
                start_datetime = today.replace(day=1)
                end_datetime = today
            elif days == "last_month":
                # Previous month
                if today.month == 1:
                    start_datetime = datetime(today.year - 1, 12, 1)
                else:
                    start_datetime = datetime(today.year, today.month - 1, 1)
                end_datetime = start_datetime + relativedelta(months=1, days=-1)
            elif days == "quarter":
                # This quarter
                current_quarter = (today.month - 1) // 3 + 1
                start_month = 3 * current_quarter - 2
                start_datetime = datetime(today.year, start_month, 1)
                end_datetime = today
            elif days == "last_quarter":
                # Previous quarter
                current_quarter = (today.month - 1) // 3 + 1
                last_quarter = current_quarter - 1 if current_quarter > 1 else 4
                year = today.year if current_quarter > 1 else today.year - 1
                start_month = 3 * last_quarter - 2
                start_datetime = datetime(year, start_month, 1)
                if last_quarter == 4:
                    end_datetime = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_datetime = datetime(year, start_month + 2, 1) + relativedelta(months=1, days=-1)
            elif days == "ytd":
                # Year to date
                start_datetime = datetime(today.year, 1, 1)
                end_datetime = today
            elif days == "last_year":
                # Last year
                start_datetime = datetime(today.year - 1, 1, 1)
                end_datetime = datetime(today.year - 1, 12, 31)
            else:
                # Last X days
                start_datetime = today - timedelta(days=days)
                end_datetime = today
        
        return start_datetime, end_datetime
    
    def create_product_filters(self):
        """Create product category filters"""
        st.sidebar.markdown("### 📊 Product Filters")
        
        all_categories = list(self.product_categories.keys()) + ['Airtime Topup']
        selected_categories = st.sidebar.multiselect(
            "Select Product Categories",
            all_categories,
            default=all_categories
        )
        
        # Get products from selected categories
        selected_products = []
        for category in selected_categories:
            if category == 'Airtime Topup':
                selected_products.append('Airtime Topup')
            else:
                selected_products.extend(self.product_categories.get(category, []))
        
        return selected_products if selected_products else self.all_products
    
    def create_metric_card(self, title, value, change=None, format_func=None):
        """Create a metric card with optional change indicator"""
        if value is None:
            display_value = "N/A"
        elif format_func:
            display_value = format_func(value)
        else:
            if isinstance(value, (int, float)):
                display_value = f"{value:,.0f}"
            else:
                display_value = str(value)
        
        html = f"""
        <div class="metric-card">
            <div class="metric-label">{title}</div>
            <div class="metric-value">{display_value}</div>
        """
        
        if change is not None:
            if change > 0:
                trend_class = "trend-up"
                trend_symbol = "↗"
            elif change < 0:
                trend_class = "trend-down"
                trend_symbol = "↘"
            else:
                trend_class = "trend-neutral"
                trend_symbol = "→"
            
            html += f"""
            <div class="{trend_class}" style="margin-top: 0.5rem;">
                {trend_symbol} {abs(change):.1f}%
            </div>
            """
        
        html += "</div>"
        return html
    
    def calculate_executive_snapshot(self, start_date, end_date, transactions_df, onboarding_df):
        """Calculate executive snapshot metrics"""
        metrics = {}
        
        # Check if we have transactions data
        if transactions_df is None or transactions_df.empty:
            metrics.update({
                'new_customers_active': 0,
                'new_customers_registered': 0,
                'new_customers_temporary': 0,
                'new_customers_total': 0,
                'active_customers': 0,
                'total_transactions': 0,
                'transaction_value': 0,
                'top_product': 'N/A',
                'top_product_count': 0,
                'success_rate': 0,
                'avg_transaction_value': 0
            })
            return metrics
        
        # Check for required columns in transactions
        if 'created_at' not in transactions_df.columns:
            st.warning("⚠️ 'created_at' column not found in transaction data")
            # Try alternative column names
            date_columns = [col for col in transactions_df.columns if 'date' in col.lower() or 'created' in col.lower()]
            if date_columns:
                transactions_df = transactions_df.rename(columns={date_columns[0]: 'created_at'})
                transactions_df['created_at'] = pd.to_datetime(transactions_df['created_at'], errors='coerce')
        
        # Filter data for the period
        try:
            if 'created_at' in transactions_df.columns:
                period_transactions = transactions_df[
                    (transactions_df['created_at'] >= start_date) & 
                    (transactions_df['created_at'] <= end_date)
                ]
            else:
                period_transactions = transactions_df
        except Exception as e:
            st.error(f"Error filtering transactions: {e}")
            period_transactions = pd.DataFrame()
        
        # Filter onboarding for the period
        if onboarding_df is not None and not onboarding_df.empty and 'registration_date' in onboarding_df.columns:
            try:
                period_onboarding = onboarding_df[
                    (onboarding_df['registration_date'] >= start_date) & 
                    (onboarding_df['registration_date'] <= end_date)
                ]
            except Exception as e:
                st.error(f"Error filtering onboarding: {e}")
                period_onboarding = pd.DataFrame()
        else:
            period_onboarding = pd.DataFrame()
        
        # New Customers by Status
        if not period_onboarding.empty and 'status' in period_onboarding.columns and 'entity' in period_onboarding.columns:
            try:
                customer_onboarding = period_onboarding[period_onboarding['entity'] == 'Customer']
                status_counts = customer_onboarding['status'].value_counts()
                metrics['new_customers_active'] = status_counts.get('Active', 0)
                metrics['new_customers_registered'] = status_counts.get('Registered', 0)
                metrics['new_customers_temporary'] = status_counts.get('TemporaryRegister', 0)
                metrics['new_customers_total'] = customer_onboarding['account_id'].nunique()
            except Exception as e:
                st.error(f"Error calculating new customers: {e}")
                metrics['new_customers_active'] = 0
                metrics['new_customers_registered'] = 0
                metrics['new_customers_temporary'] = 0
                metrics['new_customers_total'] = 0
        else:
            metrics['new_customers_active'] = 0
            metrics['new_customers_registered'] = 0
            metrics['new_customers_temporary'] = 0
            metrics['new_customers_total'] = 0
        
        # Active Customers (customers with successful transactions)
        if not period_transactions.empty and 'status' in period_transactions.columns and 'entity_name' in period_transactions.columns:
            try:
                customer_transactions = period_transactions[
                    (period_transactions['entity_name'] == 'Customer') &
                    (period_transactions['status'] == 'SUCCESS')
                ]
                
                if not customer_transactions.empty and 'user_identifier' in customer_transactions.columns:
                    user_transaction_counts = customer_transactions.groupby('user_identifier').size()
                    active_customers = user_transaction_counts[user_transaction_counts >= 2].index.tolist()
                    metrics['active_customers'] = len(active_customers)
                else:
                    metrics['active_customers'] = 0
            except Exception as e:
                st.error(f"Error calculating active customers: {e}")
                metrics['active_customers'] = 0
        else:
            metrics['active_customers'] = 0
        
        # Transaction Volume and Value
        if not period_transactions.empty and 'status' in period_transactions.columns:
            try:
                successful_transactions = period_transactions[period_transactions['status'] == 'SUCCESS']
                metrics['total_transactions'] = len(successful_transactions)
                
                if 'amount' in successful_transactions.columns:
                    metrics['transaction_value'] = successful_transactions['amount'].sum()
                else:
                    metrics['transaction_value'] = 0
            except Exception as e:
                st.error(f"Error calculating transaction metrics: {e}")
                metrics['total_transactions'] = 0
                metrics['transaction_value'] = 0
        else:
            metrics['total_transactions'] = 0
            metrics['transaction_value'] = 0
        
        # Top Product
        if not period_transactions.empty and 'status' in period_transactions.columns and 'entity_name' in period_transactions.columns and 'product_name' in period_transactions.columns:
            try:
                product_counts = period_transactions[
                    (period_transactions['status'] == 'SUCCESS') &
                    (period_transactions['entity_name'] == 'Customer')
                ]['product_name'].value_counts()
                
                if not product_counts.empty:
                    metrics['top_product'] = str(product_counts.index[0])
                    metrics['top_product_count'] = int(product_counts.iloc[0])
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['top_product_count'] = 0
            except Exception as e:
                st.error(f"Error calculating top product: {e}")
                metrics['top_product'] = 'N/A'
                metrics['top_product_count'] = 0
        else:
            metrics['top_product'] = 'N/A'
            metrics['top_product_count'] = 0
        
        # Success Rate
        if not period_transactions.empty and 'status' in period_transactions.columns:
            try:
                total_transactions = len(period_transactions)
                successful_count = len(period_transactions[period_transactions['status'] == 'SUCCESS'])
                metrics['success_rate'] = (successful_count / total_transactions * 100) if total_transactions > 0 else 0
            except:
                metrics['success_rate'] = 0
        else:
            metrics['success_rate'] = 0
        
        # Average Transaction Value
        if metrics.get('total_transactions', 0) > 0 and metrics.get('transaction_value', 0) > 0:
            metrics['avg_transaction_value'] = metrics['transaction_value'] / metrics['total_transactions']
        else:
            metrics['avg_transaction_value'] = 0
        
        return metrics
    
    def display_executive_snapshot(self, metrics):
        """Display executive snapshot metrics"""
        st.markdown('<div class="sub-header">📈 Executive Snapshot</div>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(self.create_metric_card(
                "New Customers",
                metrics.get('new_customers_total', 0)
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(self.create_metric_card(
                "Active Customers",
                metrics.get('active_customers', 0)
            ), unsafe_allow_html=True)
        
        with col3:
            st.markdown(self.create_metric_card(
                "Total Transactions",
                metrics.get('total_transactions', 0)
            ), unsafe_allow_html=True)
        
        with col4:
            st.markdown(self.create_metric_card(
                "Transaction Value",
                metrics.get('transaction_value', 0),
                format_func=lambda x: f"₦{x:,.0f}" if x else "₦0"
            ), unsafe_allow_html=True)
        
        # Additional metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(self.create_metric_card(
                "Active Status",
                metrics.get('new_customers_active', 0)
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(self.create_metric_card(
                "Registered Status",
                metrics.get('new_customers_registered', 0)
            ), unsafe_allow_html=True)
        
        with col3:
            st.markdown(self.create_metric_card(
                "Temporary Status",
                metrics.get('new_customers_temporary', 0)
            ), unsafe_allow_html=True)
        
        with col4:
            top_product = metrics.get('top_product', 'N/A')
            top_count = metrics.get('top_product_count', 0)
            product_text = f"{top_product[:15]}..." if len(str(top_product)) > 15 else top_product
            st.markdown(self.create_metric_card(
                "Top Product",
                f"{product_text} ({top_count})"
            ), unsafe_allow_html=True)
        
        # Success rate and average value
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(self.create_metric_card(
                "Success Rate",
                f"{metrics.get('success_rate', 0):.1f}%"
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(self.create_metric_card(
                "Avg Transaction Value",
                f"₦{metrics.get('avg_transaction_value', 0):,.0f}"
            ), unsafe_allow_html=True)
    
    def display_product_performance(self, transactions_df, selected_products):
        """Display product performance analysis"""
        st.markdown('<div class="sub-header">📊 Product Performance</div>', unsafe_allow_html=True)
        
        if transactions_df is None or transactions_df.empty:
            st.markdown('<div class="warning-box">⚠️ No transaction data available for the selected period</div>', unsafe_allow_html=True)
            return
        
        # Filter successful customer transactions
        if 'status' not in transactions_df.columns or 'entity_name' not in transactions_df.columns:
            st.error("Required columns (status, entity_name) not found in transaction data")
            return
        
        customer_transactions = transactions_df[
            (transactions_df['entity_name'] == 'Customer') &
            (transactions_df['status'] == 'SUCCESS')
        ]
        
        if customer_transactions.empty:
            st.info("No successful customer transactions in the selected period")
            return
        
        # Prepare product performance data
        product_data = []
        for product in selected_products:
            if product == 'Airtime Topup':
                # Check if service_name column exists
                if 'service_name' in customer_transactions.columns:
                    product_trans = customer_transactions[
                        (customer_transactions['service_name'] == 'Airtime Topup')
                    ]
                else:
                    product_trans = pd.DataFrame()
            elif product == 'Internal Wallet Transfer':
                product_trans = customer_transactions[
                    (customer_transactions['product_name'] == 'Internal Wallet Transfer')
                ]
                # Exclude fee transactions if ucp_name column exists
                if 'ucp_name' in product_trans.columns and not product_trans.empty:
                    product_trans = product_trans[
                        ~product_trans['ucp_name'].astype(str).str.contains('Fee', case=False, na=False)
                    ]
            else:
                if 'product_name' in customer_transactions.columns:
                    product_trans = customer_transactions[
                        customer_transactions['product_name'] == product
                    ]
                else:
                    product_trans = pd.DataFrame()
            
            if not product_trans.empty:
                product_data.append({
                    'Product': product,
                    'Transactions': len(product_trans),
                    'Unique Users': product_trans['user_identifier'].nunique() if 'user_identifier' in product_trans.columns else 0,
                    'Total Amount': product_trans['amount'].sum() if 'amount' in product_trans.columns else 0,
                    'Avg Amount': product_trans['amount'].mean() if 'amount' in product_trans.columns and len(product_trans) > 0 else 0
                })
        
        if product_data:
            product_df = pd.DataFrame(product_data)
            
            # Display in columns
            col1, col2 = st.columns([3, 2])
            
            with col1:
                # Transactions by product
                display_df = product_df.sort_values('Transactions', ascending=False).head(10)
                if not display_df.empty:
                    fig = px.bar(
                        display_df,
                        x='Product',
                        y='Transactions',
                        title='Top 10 Products by Transaction Count',
                        color='Transactions',
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(
                        xaxis_title="Product",
                        yaxis_title="Number of Transactions",
                        height=400,
                        showlegend=False
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No product data to display")
            
            with col2:
                # Product summary table
                st.markdown("**Product Performance Summary**")
                if not product_df.empty:
                    summary_df = product_df.sort_values('Transactions', ascending=False)
                    summary_df = summary_df[['Product', 'Transactions', 'Unique Users', 'Total Amount']].copy()
                    summary_df['Total Amount'] = summary_df['Total Amount'].apply(lambda x: f"₦{x:,.0f}" if pd.notnull(x) else "₦0")
                    st.dataframe(
                        summary_df,
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("No product summary data available")
            
            # Amount by product
            if not product_df.empty and product_df['Total Amount'].sum() > 0:
                st.markdown("---")
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    fig2 = px.pie(
                        product_df[product_df['Total Amount'] > 0],
                        values='Total Amount',
                        names='Product',
                        title='Transaction Value Distribution by Product',
                        hole=0.3
                    )
                    fig2.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig2, use_container_width=True)
                
                with col2:
                    # Key metrics
                    st.markdown("**Key Metrics**")
                    total_transactions = product_df['Transactions'].sum()
                    total_users = product_df['Unique Users'].sum()
                    avg_trans_per_user = total_transactions / total_users if total_users > 0 else 0
                    st.metric("Avg Transactions per User", f"{avg_trans_per_user:.1f}")
                    st.metric("Total Unique Product Users", f"{total_users:,.0f}")
                    st.metric("Overall Transaction Value", f"₦{product_df['Total Amount'].sum():,.0f}")
        else:
            st.info("No product performance data available for selected filters")
    
    def display_customer_acquisition(self, onboarding_df):
        """Display customer acquisition metrics"""
        st.markdown('<div class="sub-header">👥 Customer Acquisition</div>', unsafe_allow_html=True)
        
        if onboarding_df is None or onboarding_df.empty:
            st.markdown('<div class="warning-box">⚠️ No onboarding data available for the selected period</div>', unsafe_allow_html=True)
            return
        
        # Check for required columns
        if 'entity' not in onboarding_df.columns:
            st.error("'entity' column not found in onboarding data")
            return
        
        # Filter for customers only
        customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
        
        if customer_onboarding.empty:
            st.info("No customer onboarding data available")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Registration trend
            if 'registration_date' in customer_onboarding.columns:
                customer_onboarding['registration_date'] = pd.to_datetime(customer_onboarding['registration_date'], errors='coerce')
                daily_registrations = customer_onboarding.set_index('registration_date').resample('D').size()
                
                if not daily_registrations.empty:
                    fig = px.line(
                        x=daily_registrations.index,
                        y=daily_registrations.values,
                        title='Daily Customer Registrations',
                        labels={'x': 'Date', 'y': 'Registrations'}
                    )
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No registration data for trend analysis")
            else:
                st.info("Registration date data not available")
        
        with col2:
            # Status distribution
            if 'status' in customer_onboarding.columns:
                status_counts = customer_onboarding['status'].value_counts()
                if not status_counts.empty:
                    fig = px.pie(
                        values=status_counts.values,
                        names=status_counts.index,
                        title='Customer Status Distribution',
                        hole=0.3
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No status data available")
            else:
                st.info("Status data not available")
        
        with col3:
            # KYC Status
            if 'kyc_status' in customer_onboarding.columns:
                kyc_counts = customer_onboarding['kyc_status'].value_counts()
                if not kyc_counts.empty:
                    st.markdown("**KYC Status**")
                    for status, count in kyc_counts.items():
                        percentage = (count / len(customer_onboarding)) * 100 if len(customer_onboarding) > 0 else 0
                        st.progress(
                            count / len(customer_onboarding) if len(customer_onboarding) > 0 else 0,
                            text=f"{status}: {count} ({percentage:.1f}%)"
                        )
                else:
                    st.info("No KYC data available")
            else:
                st.info("KYC status data not available")
            
            # Metrics
            st.metric("Total Registrations", f"{len(customer_onboarding):,.0f}")
            if 'kyc_status' in customer_onboarding.columns:
                verified_count = customer_onboarding[
                    customer_onboarding['kyc_status'].str.upper() == 'VERIFIED'
                ].shape[0] if 'kyc_status' in customer_onboarding.columns else 0
                st.metric("KYC Verified", f"{verified_count:,}")
    
    def display_transaction_analysis(self, transactions_df):
        """Display transaction analysis"""
        st.markdown('<div class="sub-header">💳 Transaction Analysis</div>', unsafe_allow_html=True)
        
        if transactions_df is None or transactions_df.empty:
            st.markdown('<div class="warning-box">⚠️ No transaction data available for the selected period</div>', unsafe_allow_html=True)
            return
        
        # Check for required columns
        if 'status' not in transactions_df.columns:
            st.error("'status' column not found in transaction data")
            return
        
        # Filter successful transactions
        successful_transactions = transactions_df[transactions_df['status'] == 'SUCCESS']
        
        if successful_transactions.empty:
            st.info("No successful transactions in the selected period")
            return
        
        # Check for date column
        if 'created_at' not in successful_transactions.columns:
            st.error("'created_at' column not found in transaction data")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily transaction volume
            successful_transactions['created_at'] = pd.to_datetime(successful_transactions['created_at'], errors='coerce')
            daily_transactions = successful_transactions.set_index('created_at').resample('D').size()
            
            if not daily_transactions.empty:
                fig = px.line(
                    x=daily_transactions.index,
                    y=daily_transactions.values,
                    title='Daily Transaction Volume',
                    labels={'x': 'Date', 'y': 'Transactions'}
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No daily transaction data available")
        
        with col2:
            # Transaction value trend
            if 'amount' in successful_transactions.columns:
                daily_value = successful_transactions.set_index('created_at').resample('D')['amount'].sum()
                
                if not daily_value.empty:
                    fig = px.line(
                        x=daily_value.index,
                        y=daily_value.values,
                        title='Daily Transaction Value',
                        labels={'x': 'Date', 'y': 'Amount (₦)'}
                    )
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No transaction value data available")
            else:
                st.info("Amount data not available")
        
        # Success rate analysis
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'status' in transactions_df.columns:
                status_counts = transactions_df['status'].value_counts()
                success_count = status_counts.get('SUCCESS', 0)
                success_rate = (success_count / len(transactions_df)) * 100 if len(transactions_df) > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
            else:
                st.metric("Success Rate", "N/A")
        
        with col2:
            if 'amount' in successful_transactions.columns and not successful_transactions.empty:
                avg_transaction_value = successful_transactions['amount'].mean()
                st.metric("Avg Transaction Value", f"₦{avg_transaction_value:,.0f}")
            else:
                st.metric("Avg Transaction Value", "₦0")
        
        with col3:
            if 'created_at' in successful_transactions.columns and not successful_transactions.empty:
                successful_transactions['date'] = pd.to_datetime(successful_transactions['created_at']).dt.date
                daily_counts = successful_transactions.groupby('date').size()
                if not daily_counts.empty:
                    peak_date = daily_counts.idxmax()
                    peak_count = daily_counts.max()
                    st.metric("Peak Day", f"{peak_date.strftime('%b %d')}: {peak_count:,}")
                else:
                    st.metric("Peak Day", "N/A")
            else:
                st.metric("Peak Day", "N/A")
    
    def display_trend_analysis(self, transactions_df, onboarding_df):
        """Display trend analysis with fixed Plotly layout"""
        st.markdown('<div class="sub-header">📈 Trend Analysis</div>', unsafe_allow_html=True)
        
        if (transactions_df is None or transactions_df.empty) and (onboarding_df is None or onboarding_df.empty):
            st.markdown('<div class="warning-box">⚠️ Insufficient data for trend analysis</div>', unsafe_allow_html=True)
            return
        
        # Prepare transactions data
        weekly_transactions = pd.DataFrame()
        if transactions_df is not None and not transactions_df.empty and 'created_at' in transactions_df.columns:
            try:
                transactions_df['created_at'] = pd.to_datetime(transactions_df['created_at'], errors='coerce')
                transactions_df['week'] = transactions_df['created_at'].dt.to_period('W').dt.start_time
                
                if 'status' in transactions_df.columns:
                    successful_transactions = transactions_df[transactions_df['status'] == 'SUCCESS']
                    if not successful_transactions.empty:
                        weekly_transactions = successful_transactions.groupby('week').agg({
                            'amount': 'sum' if 'amount' in successful_transactions.columns else None,
                            'id': 'count' if 'id' in successful_transactions.columns else None
                        })
                        weekly_transactions.columns = ['transaction_value', 'transaction_count']
            except Exception as e:
                st.error(f"Error preparing transaction trends: {e}")
        
        # Prepare onboarding data
        weekly_registrations = pd.DataFrame()
        if onboarding_df is not None and not onboarding_df.empty and 'registration_date' in onboarding_df.columns:
            try:
                onboarding_df['registration_date'] = pd.to_datetime(onboarding_df['registration_date'], errors='coerce')
                onboarding_df['week'] = onboarding_df['registration_date'].dt.to_period('W').dt.start_time
                
                if 'entity' in onboarding_df.columns:
                    customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
                    if not customer_onboarding.empty:
                        weekly_registrations = customer_onboarding.groupby('week').size().reset_index(name='registrations')
            except Exception as e:
                st.error(f"Error preparing registration trends: {e}")
        
        # Create a simple trend chart without complex y-axis configuration
        try:
            fig = go.Figure()
            
            # Add traces based on available data
            traces_added = False
            
            if not weekly_transactions.empty and 'transaction_count' in weekly_transactions.columns:
                fig.add_trace(go.Scatter(
                    x=weekly_transactions.index,
                    y=weekly_transactions['transaction_count'],
                    name='Transaction Count',
                    line=dict(color='#1E3A8A', width=3)
                ))
                traces_added = True
            
            if not weekly_registrations.empty:
                fig.add_trace(go.Scatter(
                    x=weekly_registrations['week'],
                    y=weekly_registrations['registrations'],
                    name='New Registrations',
                    line=dict(color='#10B981', width=3, dash='dash')
                ))
                traces_added = True
            
            if not weekly_transactions.empty and 'transaction_value' in weekly_transactions.columns:
                # Create secondary axis for transaction value
                fig.add_trace(go.Scatter(
                    x=weekly_transactions.index,
                    y=weekly_transactions['transaction_value'],
                    name='Transaction Value (₦)',
                    line=dict(color='#F59E0B', width=3),
                    yaxis="y2"
                ))
                traces_added = True
            
            if traces_added:
                # Configure layout
                layout_config = {
                    'title': 'Weekly Performance Trends',
                    'xaxis': dict(title='Week'),
                    'yaxis': dict(
                        title='Count',
                        titlefont=dict(color='#1E3A8A'),
                        tickfont=dict(color='#1E3A8A')
                    ),
                    'height': 500,
                    'showlegend': True,
                    'legend': dict(
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=0.01
                    )
                }
                
                # Add secondary axis if we have transaction value
                if not weekly_transactions.empty and 'transaction_value' in weekly_transactions.columns:
                    layout_config['yaxis2'] = dict(
                        title='Transaction Value (₦)',
                        titlefont=dict(color='#F59E0B'),
                        tickfont=dict(color='#F59E0B'),
                        anchor='x',
                        overlaying='y',
                        side='right'
                    )
                
                fig.update_layout(**layout_config)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data for trend analysis")
                
        except Exception as e:
            st.error(f"Error creating trend chart: {e}")
            # Fallback to simple display
            if not weekly_transactions.empty:
                st.write("Transaction Trends:")
                st.dataframe(weekly_transactions.head())
            if not weekly_registrations.empty:
                st.write("Registration Trends:")
                st.dataframe(weekly_registrations.head())
    
    def run_dashboard(self):
        """Main method to run the dashboard"""
        # Header
        st.markdown('<div class="main-header">📊 Business Development Performance Dashboard</div>', unsafe_allow_html=True)
        
        # Sidebar filters
        with st.sidebar:
            st.markdown("### 🎯 Dashboard Filters")
            
            # Test database connection
            if st.button("🔌 Test Database Connection", use_container_width=True):
                if self.test_db_connection():
                    st.success("✅ Database connection successful!")
                else:
                    st.error("❌ Database connection failed")
            
            st.markdown("---")
            
            # Date range selection
            start_date, end_date = self.create_date_filters()
            
            # Product filters
            selected_products = self.create_product_filters()
            
            # Load data button
            st.markdown("---")
            if st.button("🚀 Load Data", type="primary", use_container_width=True, key="load_data"):
                with st.spinner("Loading data from database..."):
                    success = self.load_data_from_db(start_date, end_date)
                    if success:
                        st.success("✅ Data loaded successfully!")
            
            # Data status
            st.markdown("---")
            st.markdown("### 📊 Data Status")
            
            if st.session_state.data_loaded:
                if st.session_state.transactions is not None and not st.session_state.transactions.empty:
                    st.success(f"✅ Transactions: {len(st.session_state.transactions):,} records")
                else:
                    st.warning("⚠️ No transaction records loaded")
                
                if st.session_state.onboarding is not None and not st.session_state.onboarding.empty:
                    st.success(f"✅ Onboarding: {len(st.session_state.onboarding):,} records")
                else:
                    st.warning("⚠️ No onboarding records loaded")
            else:
                st.info("👈 Click 'Load Data' to begin analysis")
            
            # Date range display
            st.markdown("---")
            st.markdown(f"**Selected Date Range:**")
            if st.session_state.start_date and st.session_state.end_date:
                st.markdown(f"{st.session_state.start_date.strftime('%B %d, %Y')} to {st.session_state.end_date.strftime('%B %d, %Y')}")
                days_diff = (st.session_state.end_date - st.session_state.start_date).days + 1
                st.markdown(f"**Duration:** {days_diff} days")
        
        # Main content area
        if st.session_state.data_loaded:
            # Check if we have any data
            has_transactions = st.session_state.transactions is not None and not st.session_state.transactions.empty
            has_onboarding = st.session_state.onboarding is not None and not st.session_state.onboarding.empty
            
            if not has_transactions and not has_onboarding:
                st.markdown('<div class="warning-box">⚠️ No data available for the selected date range. Try selecting a different date range.</div>', unsafe_allow_html=True)
                return
            
            # Executive Snapshot
            metrics = self.calculate_executive_snapshot(
                start_date, end_date,
                st.session_state.transactions,
                st.session_state.onboarding
            )
            self.display_executive_snapshot(metrics)
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Product Performance
            self.display_product_performance(st.session_state.transactions, selected_products)
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Customer Acquisition (only if we have onboarding data)
            if has_onboarding:
                self.display_customer_acquisition(st.session_state.onboarding)
                st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Transaction Analysis (only if we have transaction data)
            if has_transactions:
                self.display_transaction_analysis(st.session_state.transactions)
                st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Trend Analysis (if we have both datasets)
            if has_transactions or has_onboarding:
                self.display_trend_analysis(st.session_state.transactions, st.session_state.onboarding)
            
            # Export options
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            st.markdown("### 📥 Export Data")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if has_transactions:
                    csv_transactions = st.session_state.transactions.to_csv(index=False)
                    st.download_button(
                        label="Download Transaction Data (CSV)",
                        data=csv_transactions,
                        file_name=f"transactions_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No transaction data to export")
            
            with col2:
                if has_onboarding:
                    csv_onboarding = st.session_state.onboarding.to_csv(index=False)
                    st.download_button(
                        label="Download Onboarding Data (CSV)",
                        data=csv_onboarding,
                        file_name=f"onboarding_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No onboarding data to export")
            
            # Data preview
            with st.expander("🔍 Preview Raw Data"):
                tab1, tab2 = st.tabs(["Transaction Data", "Onboarding Data"])
                
                with tab1:
                    if has_transactions:
                        st.dataframe(
                            st.session_state.transactions.head(100),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info("No transaction data available")
                
                with tab2:
                    if has_onboarding:
                        st.dataframe(
                            st.session_state.onboarding.head(100),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info("No onboarding data available")
        
        else:
            # Welcome/instructions
            st.markdown('<div class="info-box">👋 Welcome to the Business Development Performance Dashboard!</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🚀 Quick Start")
                st.markdown("""
                1. **Test database connection** in sidebar
                2. **Select date range** (try Last 90 Days)
                3. **Choose product categories** to analyze
                4. **Click 'Load Data'** to fetch from MySQL
                5. **Explore analytics** in main dashboard
                """)
            
            with col2:
                st.markdown("### 🔧 Features")
                st.markdown("""
                - 📈 **Executive Snapshot**: Key metrics
                - 📊 **Product Performance**: Detailed analysis
                - 👥 **Customer Acquisition**: Registration analytics
                - 💳 **Transaction Analysis**: Volume & value
                - 📈 **Trend Analysis**: Weekly performance
                - 📥 **Export Options**: Download CSV data
                """)
            
            # Date range examples
            st.markdown("### 📅 Available Date Ranges")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**Short-term:**")
                st.markdown("- Last 7 Days")
                st.markdown("- Last 30 Days")
                st.markdown("- Last 90 Days")
            
            with col2:
                st.markdown("**Monthly:**")
                st.markdown("- This Month")
                st.markdown("- Last Month")
            
            with col3:
                st.markdown("**Quarterly/Yearly:**")
                st.markdown("- This Quarter")
                st.markdown("- Last Quarter")
                st.markdown("- Year to Date")
                st.markdown("- Last Year")
                st.markdown("- Custom Range")

# Main execution
def main():
    # Initialize dashboard
    dashboard = PerformanceDashboard()
    
    # Run dashboard
    dashboard.run_dashboard()

if __name__ == "__main__":
    main()
