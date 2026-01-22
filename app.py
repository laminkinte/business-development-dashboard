import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
import json
import io
from pathlib import Path

warnings.filterwarnings('ignore')

# Set page configuration
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
        font-weight: 700;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #1E3A8A;
        font-weight: 600;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #FFFFFF;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #1E3A8A;
    }
    .product-card {
        background-color: #F8FAFC;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
        border-left: 3px solid #3B82F6;
    }
    .positive {
        color: #10B981;
        font-weight: 600;
    }
    .negative {
        color: #EF4444;
        font-weight: 600;
    }
    .neutral {
        color: #6B7280;
        font-weight: 600;
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
        
        # Initialize data containers
        self.transactions = None
        self.onboarding = None
        
    def connect_to_mysql(self, host, port, user, password, database):
        """Establish MySQL connection"""
        try:
            connection = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except Exception as e:
            st.error(f"Error connecting to MySQL: {e}")
            return None
    
    def load_transactions_from_mysql(self, connection, start_date=None, end_date=None):
        """Load transaction data from MySQL"""
        try:
            with connection.cursor() as cursor:
                # Build query with date filters
                query = """
                SELECT 
                    id,
                    user_identifier,
                    transaction_id,
                    sub_transaction_id,
                    entity_name,
                    full_name,
                    created_by,
                    status,
                    internal_status,
                    service_name,
                    product_name,
                    transaction_type,
                    amount,
                    before_balance,
                    after_balance,
                    ucp_name,
                    wallet_name,
                    pouch_name,
                    reference,
                    error_code,
                    error_message,
                    vendor_transaction_id,
                    vendor_response_code,
                    vendor_message,
                    slug,
                    remarks,
                    created_at,
                    business_hierarchy,
                    parent_user_identifier,
                    parent_full_name
                FROM transaction_table
                WHERE 1=1
                """
                
                params = []
                
                if start_date:
                    query += " AND created_at >= %s"
                    params.append(start_date)
                
                if end_date:
                    query += " AND created_at <= %s"
                    params.append(end_date)
                
                query += " ORDER BY created_at DESC"
                
                cursor.execute(query, params)
                result = cursor.fetchall()
                
                if result:
                    df = pd.DataFrame(result)
                    
                    # Convert date columns
                    if 'created_at' in df.columns:
                        df['created_at'] = pd.to_datetime(df['created_at'])
                    
                    # Clean column names (strip whitespace)
                    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
                    
                    # Clean numeric columns
                    if 'amount' in df.columns:
                        df['amount'] = pd.to_numeric(df['amount'].astype(str)
                            .str.replace(',', '')
                            .str.replace(' ', '')
                            .str.replace('D', '')
                            .str.replace('GMD', '')
                            .str.replace('$', '')
                            .str.replace('€', ''), 
                            errors='coerce')
                    
                    # Clean text columns
                    text_columns = ['user_identifier', 'product_name', 'entity_name', 'transaction_type', 
                                  'ucp_name', 'service_name', 'status', 'sub_transaction_id']
                    for col in text_columns:
                        if col in df.columns:
                            df[col] = df[col].astype(str).str.strip()
                    
                    return df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            st.error(f"Error loading transaction data: {e}")
            return pd.DataFrame()
    
    def load_onboarding_from_mysql(self, connection, start_date=None, end_date=None):
        """Load onboarding data from MySQL"""
        try:
            with connection.cursor() as cursor:
                # Build query with date filters
                query = """
                SELECT 
                    account_id,
                    full_name,
                    mobile,
                    email,
                    region,
                    district,
                    town_village,
                    business_name,
                    kyc_status,
                    registration_date,
                    updated_at,
                    proof_of_id,
                    identification_number,
                    customer_referrer_code,
                    customer_referrer_mobile,
                    referrer_entity,
                    entity,
                    bank,
                    bank_account_name,
                    bank_account_number,
                    status
                FROM onboarding_table
                WHERE entity = 'Customer'
                """
                
                params = []
                
                if start_date:
                    query += " AND registration_date >= %s"
                    params.append(start_date)
                
                if end_date:
                    query += " AND registration_date <= %s"
                    params.append(end_date)
                
                cursor.execute(query, params)
                result = cursor.fetchall()
                
                if result:
                    df = pd.DataFrame(result)
                    
                    # Convert date columns
                    date_columns = ['registration_date', 'updated_at']
                    for col in date_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col])
                    
                    # Clean column names
                    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
                    
                    # Create User Identifier for merging
                    if 'mobile' in df.columns:
                        df['user_identifier'] = df['mobile'].astype(str).str.strip()
                    
                    return df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            st.error(f"Error loading onboarding data: {e}")
            return pd.DataFrame()
    
    def get_new_registered_customers_segmented(self, start_date, end_date):
        """Get new registered customers segmented by Status"""
        if self.onboarding is None or self.onboarding.empty:
            return {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}, {}
        
        period_onboarding = self.onboarding[
            (self.onboarding['registration_date'] >= start_date) & 
            (self.onboarding['registration_date'] <= end_date)
        ].copy()
        
        segmented_counts = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        customer_lists = {'Active': [], 'Registered': [], 'TemporaryRegister': [], 'Total': []}
        
        if not period_onboarding.empty:
            # Filter customers with status in ['Active', 'Registered', 'TemporaryRegister']
            valid_statuses = ['Active', 'Registered', 'TemporaryRegister']
            valid_customers = period_onboarding[
                period_onboarding['status'].isin(valid_statuses)
            ]
            
            # Segment by status
            for status in valid_statuses:
                status_customers = valid_customers[valid_customers['status'] == status]
                segmented_counts[status] = status_customers['user_identifier'].nunique()
                customer_lists[status] = status_customers['user_identifier'].unique().tolist()
            
            # Total
            segmented_counts['Total'] = valid_customers['user_identifier'].nunique()
            customer_lists['Total'] = valid_customers['user_identifier'].unique().tolist()
        
        return segmented_counts, customer_lists
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type='custom'):
        """Calculate Executive Snapshot metrics WITH SEGMENTED CUSTOMERS"""
        metrics = {}
        
        # Get new registered customers SEGMENTED BY STATUS
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        metrics['new_customers_active'] = segmented_counts['Active']
        metrics['new_customers_registered'] = segmented_counts['Registered']
        metrics['new_customers_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_customers_total'] = segmented_counts['Total']
        
        # Get ALL active customers
        if self.transactions is not None and not self.transactions.empty:
            period_transactions = self.transactions[
                (self.transactions['created_at'] >= start_date) & 
                (self.transactions['created_at'] <= end_date)
            ]
            
            if not period_transactions.empty:
                # Filter successful customer transactions
                customer_transactions = period_transactions[
                    (period_transactions['entity_name'] == 'Customer') &
                    (period_transactions['status'] == 'SUCCESS')
                ]
                
                if not customer_transactions.empty:
                    # Count transactions per user
                    user_transaction_counts = customer_transactions.groupby('user_identifier').size()
                    
                    # Different thresholds for different period types
                    if period_type in ['weekly', 'rolling', 'custom']:
                        threshold = 2
                    else:  # monthly
                        threshold = 10
                    
                    active_users = user_transaction_counts[user_transaction_counts >= threshold].index.tolist()
                    metrics['active_customers_all'] = len(active_users)
                else:
                    metrics['active_customers_all'] = 0
            else:
                metrics['active_customers_all'] = 0
        else:
            metrics['active_customers_all'] = 0
        
        # Get top and lowest performing products
        if self.transactions is not None and not self.transactions.empty:
            period_transactions = self.transactions[
                (self.transactions['created_at'] >= start_date) & 
                (self.transactions['created_at'] <= end_date)
            ]
            
            if not period_transactions.empty:
                # Filter to customer transactions
                customer_transactions = period_transactions[
                    (period_transactions['entity_name'] == 'Customer') &
                    (period_transactions['status'] == 'SUCCESS') &
                    (period_transactions['product_name'].notna())
                ]
                
                if not customer_transactions.empty:
                    product_counts_dict = {}
                    product_users_dict = {}
                    product_amount_dict = {}
                    
                    for product in customer_transactions['product_name'].unique():
                        if product == 'Internal Wallet Transfer':
                            # CORRECTED P2P COUNTING:
                            p2p_transactions = customer_transactions[
                                (customer_transactions['product_name'] == 'Internal Wallet Transfer') &
                                (customer_transactions['transaction_type'] == 'DR')
                            ]
                            
                            # Exclude fee transactions
                            if 'ucp_name' in p2p_transactions.columns:
                                p2p_transactions = p2p_transactions[
                                    ~p2p_transactions['ucp_name'].str.contains('Fee', case=False, na=False)
                                ]
                            
                            product_counts_dict[product] = len(p2p_transactions)
                            product_users_dict[product] = p2p_transactions['user_identifier'].nunique()
                            product_amount_dict[product] = p2p_transactions['amount'].sum() if 'amount' in p2p_transactions.columns else 0
                        else:
                            # For other products, count all transactions
                            product_transactions = customer_transactions[
                                customer_transactions['product_name'] == product
                            ]
                            product_counts_dict[product] = len(product_transactions)
                            product_users_dict[product] = product_transactions['user_identifier'].nunique()
                            product_amount_dict[product] = product_transactions['amount'].sum() if 'amount' in product_transactions.columns else 0
                    
                    # Also include Airtime Topup as a service
                    if 'Airtime Topup' in self.services:
                        airtime_transactions = period_transactions[
                            (period_transactions['service_name'] == 'Airtime Topup') &
                            (period_transactions['entity_name'] == 'Customer') &
                            (period_transactions['status'] == 'SUCCESS') &
                            (period_transactions['transaction_type'] == 'DR')
                        ]
                        product_counts_dict['Airtime Topup'] = len(airtime_transactions)
                        product_users_dict['Airtime Topup'] = airtime_transactions['user_identifier'].nunique()
                        product_amount_dict['Airtime Topup'] = airtime_transactions['amount'].sum() if 'amount' in airtime_transactions.columns else 0
                    
                    # Convert to Series for sorting
                    product_counts = pd.Series(product_counts_dict)
                    product_counts = product_counts.sort_values(ascending=False)
                    
                    if not product_counts.empty:
                        # Get top performing product
                        top_product = product_counts.index[0]
                        top_product_count = int(product_counts.iloc[0])
                        
                        # Get lowest performing product (with at least 1 transaction)
                        active_products = product_counts[product_counts > 0]
                        if not active_products.empty:
                            low_product = active_products.index[-1]
                            low_product_count = int(active_products.iloc[-1])
                        else:
                            low_product = 'N/A'
                            low_product_count = 0
                        
                        metrics['top_product'] = top_product
                        metrics['top_product_count'] = top_product_count
                        metrics['top_product_users'] = product_users_dict.get(top_product, 0)
                        metrics['top_product_amount'] = product_amount_dict.get(top_product, 0)
                        
                        metrics['low_product'] = low_product
                        metrics['low_product_count'] = low_product_count
                        metrics['low_product_users'] = product_users_dict.get(low_product, 0)
                        metrics['low_product_amount'] = product_amount_dict.get(low_product, 0)
                    else:
                        metrics['top_product'] = 'N/A'
                        metrics['low_product'] = 'N/A'
        
        return metrics

def main():
    # Initialize session state
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'transactions' not in st.session_state:
        st.session_state.transactions = None
    if 'onboarding' not in st.session_state:
        st.session_state.onboarding = None
    if 'generator' not in st.session_state:
        st.session_state.generator = PerformanceReportGenerator()
    
    # Title and header
    st.markdown('<h1 class="main-header">📊 Business Development Performance Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar for filters and controls
    with st.sidebar:
        st.markdown("### 🛠️ Configuration")
        
        # Database Connection Section
        st.markdown("#### 🔗 Database Connection")
        with st.expander("MySQL Configuration", expanded=True):
            host = st.text_input("Host", value="localhost")
            port = st.number_input("Port", value=3306, min_value=1, max_value=65535)
            user = st.text_input("Username", value="root")
            password = st.text_input("Password", type="password", value="")
            database = st.text_input("Database", value="your_database")
        
        # Date Range Selection
        st.markdown("#### 📅 Date Range Selection")
        
        date_option = st.radio(
            "Select Date Range Type",
            ["Custom Range", "Preset Periods", "Rolling Periods"],
            index=0
        )
        
        if date_option == "Custom Range":
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=datetime(2025, 10, 1))
            with col2:
                end_date = st.date_input("End Date", value=min(datetime(2026, 1, 14), datetime.now()))
            
            period_type = "custom"
            
        elif date_option == "Preset Periods":
            period_type = st.selectbox(
                "Select Period",
                [
                    "October 2025 (Monthly)",
                    "November 2025 (Monthly)",
                    "December 2025 (Monthly)",
                    "January 2026 - Week 1 (Jan 1-7)",
                    "January 2026 - Week 2 (Jan 8-14)"
                ]
            )
            
            # Map period selection to dates
            period_dates = {
                "October 2025 (Monthly)": (datetime(2025, 10, 1), datetime(2025, 10, 31), "monthly"),
                "November 2025 (Monthly)": (datetime(2025, 11, 1), datetime(2025, 11, 30), "monthly"),
                "December 2025 (Monthly)": (datetime(2025, 12, 1), datetime(2025, 12, 31), "monthly"),
                "January 2026 - Week 1 (Jan 1-7)": (datetime(2026, 1, 1), datetime(2026, 1, 7), "weekly"),
                "January 2026 - Week 2 (Jan 8-14)": (datetime(2026, 1, 8), datetime(2026, 1, 14), "weekly")
            }
            
            start_date, end_date, period_type = period_dates[period_type]
            start_date = start_date.date()
            end_date = end_date.date()
            
        else:  # Rolling Periods
            rolling_option = st.selectbox(
                "Select Rolling Period",
                [
                    "Last 7 Days",
                    "Last 14 Days",
                    "Last 30 Days",
                    "Last 90 Days"
                ]
            )
            
            days_map = {
                "Last 7 Days": 7,
                "Last 14 Days": 14,
                "Last 30 Days": 30,
                "Last 90 Days": 90
            }
            
            days = days_map[rolling_option]
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days-1)
            period_type = "rolling"
        
        # Convert to datetime
        start_date_dt = datetime.combine(start_date, datetime.min.time())
        end_date_dt = datetime.combine(end_date, datetime.max.time())
        
        # Load Data Button
        st.markdown("---")
        if st.button("📥 Load Data from MySQL", type="primary", use_container_width=True):
            with st.spinner("Connecting to MySQL and loading data..."):
                # Initialize generator
                generator = st.session_state.generator
                
                # Connect to MySQL
                connection = generator.connect_to_mysql(host, port, user, password, database)
                
                if connection:
                    # Load transactions
                    st.info("Loading transaction data...")
                    transactions_df = generator.load_transactions_from_mysql(
                        connection, 
                        start_date_dt,
                        end_date_dt
                    )
                    
                    # Load onboarding
                    st.info("Loading onboarding data...")
                    onboarding_df = generator.load_onboarding_from_mysql(
                        connection,
                        start_date_dt,
                        end_date_dt
                    )
                    
                    connection.close()
                    
                    if not transactions_df.empty and not onboarding_df.empty:
                        generator.transactions = transactions_df
                        generator.onboarding = onboarding_df
                        
                        st.session_state.transactions = transactions_df
                        st.session_state.onboarding = onboarding_df
                        st.session_state.data_loaded = True
                        st.session_state.generator = generator
                        
                        st.success(f"✅ Loaded {len(transactions_df):,} transactions and {len(onboarding_df):,} onboarding records")
                    else:
                        st.error("❌ No data found for the selected period")
                else:
                    st.error("❌ Failed to connect to MySQL")
        
        # Upload CSV Files (alternative)
        st.markdown("---")
        st.markdown("#### 📁 Alternative: Upload CSV Files")
        
        uploaded_transactions = st.file_uploader("Upload Transactions CSV", type=['csv'])
        uploaded_onboarding = st.file_uploader("Upload Onboarding CSV", type=['csv'])
        
        if uploaded_transactions and uploaded_onboarding:
            if st.button("📥 Load from CSV Files", use_container_width=True):
                with st.spinner("Loading CSV files..."):
                    try:
                        generator = st.session_state.generator
                        
                        # Load transaction data
                        transactions_df = pd.read_csv(uploaded_transactions, low_memory=False, encoding='utf-8')
                        transactions_df.columns = transactions_df.columns.str.strip()
                        
                        # Parse dates
                        if 'created_at' in transactions_df.columns:
                            transactions_df['created_at'] = pd.to_datetime(transactions_df['created_at'], errors='coerce')
                        
                        # Filter by date range
                        mask = (
                            (transactions_df['created_at'] >= start_date_dt) & 
                            (transactions_df['created_at'] <= end_date_dt) &
                            transactions_df['created_at'].notna()
                        )
                        transactions_df = transactions_df[mask].copy()
                        
                        # Load onboarding data
                        onboarding_df = pd.read_csv(uploaded_onboarding, low_memory=False, encoding='utf-8')
                        onboarding_df.columns = onboarding_df.columns.str.replace('\ufeff', '').str.strip()
                        
                        # Parse dates
                        for date_col in ['registration_date', 'updated_at']:
                            if date_col in onboarding_df.columns:
                                onboarding_df[date_col] = pd.to_datetime(onboarding_df[date_col], errors='coerce')
                        
                        # Filter by date range
                        if 'registration_date' in onboarding_df.columns:
                            reg_mask = (
                                (onboarding_df['registration_date'] >= start_date_dt) & 
                                (onboarding_df['registration_date'] <= end_date_dt) &
                                onboarding_df['registration_date'].notna()
                            )
                            onboarding_df = onboarding_df[reg_mask].copy()
                        
                        generator.transactions = transactions_df
                        generator.onboarding = onboarding_df
                        
                        st.session_state.transactions = transactions_df
                        st.session_state.onboarding = onboarding_df
                        st.session_state.data_loaded = True
                        st.session_state.generator = generator
                        
                        st.success(f"✅ Loaded {len(transactions_df):,} transactions and {len(onboarding_df):,} onboarding records")
                        
                    except Exception as e:
                        st.error(f"Error loading CSV files: {e}")
        
        # Data Summary
        if st.session_state.data_loaded:
            st.markdown("---")
            st.markdown("#### 📊 Data Summary")
            st.write(f"**Transactions:** {len(st.session_state.transactions):,}")
            st.write(f"**Onboarding Records:** {len(st.session_state.onboarding):,}")
            st.write(f"**Period:** {start_date_dt.strftime('%b %d, %Y')} to {end_date_dt.strftime('%b %d, %Y')}")
    
    # Main content area
    if st.session_state.data_loaded:
        generator = st.session_state.generator
        
        # Calculate metrics
        with st.spinner("Calculating metrics..."):
            metrics = generator.calculate_executive_snapshot(start_date_dt, end_date_dt, period_type)
        
        # Executive Snapshot Section
        st.markdown('<h2 class="sub-header">📈 Executive Snapshot</h2>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                label="New Customers (Total)",
                value=f"{metrics.get('new_customers_total', 0):,}",
                delta=None
            )
            st.caption(f"Active: {metrics.get('new_customers_active', 0):,}")
            st.caption(f"Registered: {metrics.get('new_customers_registered', 0):,}")
            st.caption(f"Temporary: {metrics.get('new_customers_temporary', 0):,}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                label="Active Customers",
                value=f"{metrics.get('active_customers_all', 0):,}",
                delta=None
            )
            st.caption(f"Threshold: {'≥2' if period_type in ['weekly', 'rolling', 'custom'] else '≥10'} transactions")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            top_product = metrics.get('top_product', 'N/A')
            top_count = metrics.get('top_product_count', 0)
            st.metric(
                label="Top Performing Product",
                value=top_product,
                delta=None
            )
            st.caption(f"Transactions: {top_count:,}")
            st.caption(f"Users: {metrics.get('top_product_users', 0):,}")
            st.caption(f"Amount: {metrics.get('top_product_amount', 0):,.2f}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            low_product = metrics.get('low_product', 'N/A')
            low_count = metrics.get('low_product_count', 0)
            st.metric(
                label="Lowest Performing Product",
                value=low_product,
                delta=None
            )
            st.caption(f"Transactions: {low_count:,}")
            st.caption(f"Users: {metrics.get('low_product_users', 0):,}")
            st.caption(f"Amount: {metrics.get('low_product_amount', 0):,.2f}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Product Performance Section
        st.markdown('<h2 class="sub-header">📊 Product Performance Analysis</h2>', unsafe_allow_html=True)
        
        # Product usage analysis
        if generator.transactions is not None and not generator.transactions.empty:
            period_transactions = generator.transactions[
                (generator.transactions['created_at'] >= start_date_dt) & 
                (generator.transactions['created_at'] <= end_date_dt)
            ]
            
            if not period_transactions.empty:
                # Get product usage counts
                product_data = []
                
                for product in generator.all_products:
                    if product == 'Internal Wallet Transfer':
                        product_trans = period_transactions[
                            (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                            (period_transactions['entity_name'] == 'Customer') &
                            (period_transactions['status'] == 'SUCCESS') &
                            (period_transactions['transaction_type'] == 'DR')
                        ]
                        
                        if 'ucp_name' in product_trans.columns:
                            product_trans = product_trans[
                                ~product_trans['ucp_name'].str.contains('Fee', case=False, na=False)
                            ]
                            
                    elif product == 'Airtime Topup':
                        product_trans = period_transactions[
                            (period_transactions['service_name'] == 'Airtime Topup') &
                            (period_transactions['entity_name'] == 'Customer') &
                            (period_transactions['status'] == 'SUCCESS') &
                            (period_transactions['transaction_type'] == 'DR')
                        ]
                    else:
                        product_trans = period_transactions[
                            (period_transactions['product_name'] == product) &
                            (period_transactions['entity_name'] == 'Customer') &
                            (period_transactions['status'] == 'SUCCESS')
                        ]
                    
                    total_transactions = len(product_trans)
                    total_users = product_trans['user_identifier'].nunique() if not product_trans.empty else 0
                    total_amount = product_trans['amount'].sum() if not product_trans.empty and 'amount' in product_trans.columns else 0
                    
                    # Find category
                    category = None
                    for cat, prods in generator.product_categories.items():
                        if product in prods:
                            category = cat
                            break
                    if product == 'Airtime Topup':
                        category = 'Airtime Topup'
                    
                    product_data.append({
                        'Product': product,
                        'Category': category,
                        'Transactions': total_transactions,
                        'Users': total_users,
                        'Amount': total_amount,
                        'Avg per Transaction': total_amount / total_transactions if total_transactions > 0 else 0
                    })
                
                product_df = pd.DataFrame(product_data)
                product_df = product_df.sort_values('Transactions', ascending=False)
                
                # Display product performance
                tab1, tab2, tab3 = st.tabs(["📋 Table View", "📊 Chart View", "📈 Trends"])
                
                with tab1:
                    # Format the dataframe
                    display_df = product_df.copy()
                    display_df['Amount'] = display_df['Amount'].apply(lambda x: f"{x:,.2f}")
                    display_df['Avg per Transaction'] = display_df['Avg per Transaction'].apply(lambda x: f"{x:,.2f}")
                    
                    st.dataframe(
                        display_df,
                        column_config={
                            "Product": st.column_config.TextColumn("Product", width="medium"),
                            "Category": st.column_config.TextColumn("Category", width="medium"),
                            "Transactions": st.column_config.NumberColumn("Transactions", format="%d"),
                            "Users": st.column_config.NumberColumn("Users", format="%d"),
                            "Amount": st.column_config.TextColumn("Amount"),
                            "Avg per Transaction": st.column_config.TextColumn("Avg/Txn")
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                
                with tab2:
                    # Create bar chart for top products
                    top_n = st.slider("Number of products to show", 5, 20, 10)
                    chart_df = product_df.head(top_n).copy()
                    
                    fig = make_subplots(
                        rows=2, cols=2,
                        subplot_titles=("Transactions by Product", "Users by Product", 
                                      "Amount by Product", "Average per Transaction"),
                        vertical_spacing=0.15,
                        horizontal_spacing=0.15
                    )
                    
                    # Transactions chart
                    fig.add_trace(
                        go.Bar(
                            x=chart_df['Product'],
                            y=chart_df['Transactions'],
                            name="Transactions",
                            marker_color='#1E3A8A'
                        ),
                        row=1, col=1
                    )
                    
                    # Users chart
                    fig.add_trace(
                        go.Bar(
                            x=chart_df['Product'],
                            y=chart_df['Users'],
                            name="Users",
                            marker_color='#3B82F6'
                        ),
                        row=1, col=2
                    )
                    
                    # Amount chart
                    fig.add_trace(
                        go.Bar(
                            x=chart_df['Product'],
                            y=chart_df['Amount'],
                            name="Amount",
                            marker_color='#10B981'
                        ),
                        row=2, col=1
                    )
                    
                    # Avg per transaction chart
                    fig.add_trace(
                        go.Bar(
                            x=chart_df['Product'],
                            y=chart_df['Avg per Transaction'],
                            name="Avg per Transaction",
                            marker_color='#8B5CF6'
                        ),
                        row=2, col=2
                    )
                    
                    fig.update_layout(
                        height=600,
                        showlegend=False,
                        title_text="Product Performance Metrics",
                        title_font_size=20
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                with tab3:
                    # Time series analysis
                    st.write("Transaction trends over time")
                    
                    # Create daily transaction counts
                    daily_transactions = period_transactions.copy()
                    daily_transactions['date'] = daily_transactions['created_at'].dt.date
                    
                    daily_counts = daily_transactions.groupby('date').size().reset_index(name='count')
                    
                    fig = px.line(
                        daily_counts,
                        x='date',
                        y='count',
                        title='Daily Transaction Volume',
                        labels={'date': 'Date', 'count': 'Number of Transactions'}
                    )
                    
                    fig.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Transactions",
                        hovermode='x unified'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
        
        # Customer Analysis Section
        st.markdown('<h2 class="sub-header">👥 Customer Analysis</h2>', unsafe_allow_html=True)
        
        if generator.onboarding is not None and not generator.onboarding.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Customer segmentation by status
                status_counts = generator.onboarding['status'].value_counts()
                
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Customer Status Distribution",
                    hole=0.4
                )
                
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # KYC status
                if 'kyc_status' in generator.onboarding.columns:
                    kyc_counts = generator.onboarding['kyc_status'].value_counts()
                    
                    fig = px.bar(
                        x=kyc_counts.index,
                        y=kyc_counts.values,
                        title="KYC Status Distribution",
                        labels={'x': 'KYC Status', 'y': 'Count'},
                        color=kyc_counts.values,
                        color_continuous_scale='Blues'
                    )
                    
                    fig.update_layout(
                        xaxis_title="KYC Status",
                        yaxis_title="Count",
                        coloraxis_showscale=False
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
        
        # Data Export Section
        st.markdown('<h2 class="sub-header">📤 Export Data</h2>', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📄 Export to Excel", use_container_width=True):
                # Create a simple export
                try:
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        # Export summary
                        summary_data = {
                            'Metric': ['Period Start', 'Period End', 'Total Transactions', 
                                     'Total Onboarding', 'New Customers', 'Active Customers',
                                     'Top Product', 'Low Product'],
                            'Value': [
                                start_date_dt.strftime('%Y-%m-%d'),
                                end_date_dt.strftime('%Y-%m-%d'),
                                len(generator.transactions),
                                len(generator.onboarding),
                                metrics.get('new_customers_total', 0),
                                metrics.get('active_customers_all', 0),
                                metrics.get('top_product', 'N/A'),
                                metrics.get('low_product', 'N/A')
                            ]
                        }
                        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                        
                        # Export product performance
                        if 'product_df' in locals():
                            product_df.to_excel(writer, sheet_name='Product Performance', index=False)
                        
                        # Export transaction sample
                        generator.transactions.head(10000).to_excel(writer, sheet_name='Transactions Sample', index=False)
                    
                    output.seek(0)
                    
                    st.download_button(
                        label="⬇️ Download Excel Report",
                        data=output,
                        file_name=f"performance_report_{start_date_dt.strftime('%Y%m%d')}_{end_date_dt.strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    
                except Exception as e:
                    st.error(f"Error creating Excel file: {e}")
        
        with col2:
            if st.button("📊 Export Charts as PDF", use_container_width=True):
                st.info("PDF export feature coming soon!")
        
        with col3:
            if st.button("🔄 Refresh Analysis", use_container_width=True):
                st.rerun()
        
        # Raw Data Preview
        with st.expander("🔍 View Raw Data Preview"):
            tab1, tab2 = st.tabs(["Transactions", "Onboarding"])
            
            with tab1:
                if generator.transactions is not None:
                    st.dataframe(
                        generator.transactions.head(100),
                        use_container_width=True,
                        hide_index=True
                    )
            
            with tab2:
                if generator.onboarding is not None:
                    st.dataframe(
                        generator.onboarding.head(100),
                        use_container_width=True,
                        hide_index=True
                    )
    
    else:
        # Welcome screen when no data is loaded
        st.markdown("""
        <div style='text-align: center; padding: 50px; background-color: #F8FAFC; border-radius: 10px;'>
            <h2 style='color: #1E3A8A;'>Welcome to the Business Development Performance Dashboard</h2>
            <p style='font-size: 18px; color: #4B5563; margin-bottom: 30px;'>
                This dashboard provides comprehensive analysis of business development and marketing performance.
            </p>
            
            <div style='display: flex; justify-content: center; gap: 30px; margin-top: 40px;'>
                <div style='flex: 1; max-width: 300px;'>
                    <div style='font-size: 48px; color: #1E3A8A;'>📊</div>
                    <h3>Performance Metrics</h3>
                    <p>Track key performance indicators across different time periods</p>
                </div>
                
                <div style='flex: 1; max-width: 300px;'>
                    <div style='font-size: 48px; color: #1E3A8A;'>🔍</div>
                    <h3>Flexible Filters</h3>
                    <p>Choose any date range with custom, preset, or rolling periods</p>
                </div>
                
                <div style='flex: 1; max-width: 300px;'>
                    <div style='font-size: 48px; color: #1E3A8A;'>📈</div>
                    <h3>Product Analysis</h3>
                    <p>Analyze product usage, penetration, and consistency</p>
                </div>
            </div>
            
            <div style='margin-top: 50px;'>
                <h3 style='color: #1E3A8A;'>To get started:</h3>
                <ol style='text-align: left; display: inline-block;'>
                    <li>Configure MySQL connection in the sidebar</li>
                    <li>Select your desired date range</li>
                    <li>Click "Load Data from MySQL"</li>
                    <li>Or upload CSV files as an alternative</li>
                </ol>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Quick stats if available in sample data
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.info("**Flexible Date Filtering**\n\nChoose any duration - custom ranges, preset periods, or rolling windows")
        
        with col2:
            st.info("**Professional Design**\n\nClean, modern interface with comprehensive visualizations")
        
        with col3:
            st.info("**MySQL Integration**\n\nConnect directly to your database or upload CSV files")

if __name__ == "__main__":
    main()
