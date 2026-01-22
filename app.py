import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql
import plotly.graph_objects as go
import plotly.express as px
import warnings
from io import BytesIO
import base64
import time

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
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #1E3A8A;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #FFFFFF;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .positive {
        color: #10B981;
        font-weight: bold;
    }
    .negative {
        color: #EF4444;
        font-weight: bold;
    }
    .neutral {
        color: #6B7280;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Database configuration (hardcoded)
DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'lamin_d_kinteh',
    'password': 'Lamin@123',
    'database': 'bdp_report',
    'port': 3306
}

class PerformanceReportGenerator:
    def __init__(self):
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
        
        self.transactions = pd.DataFrame()
        self.onboarding = pd.DataFrame()
        self.data_cache = {}
    
    def connect_to_mysql(self):
        """Connect to MySQL database"""
        try:
            connection = pymysql.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                port=DB_CONFIG.get('port', 3306),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            return connection
        except Exception as e:
            st.error(f"❌ Error connecting to MySQL: {str(e)}")
            return None
    
    def get_database_date_range(self):
        """Get the actual date range from database"""
        try:
            connection = self.connect_to_mysql()
            if connection is None:
                return None
            
            with connection.cursor() as cursor:
                cursor.execute("SELECT MIN(created_at) as min_date, MAX(created_at) as max_date FROM Transaction")
                trans_range = cursor.fetchone()
                
                cursor.execute("SELECT MIN(registration_date) as min_date, MAX(registration_date) as max_date FROM Onboarding")
                onboard_range = cursor.fetchone()
            
            connection.close()
            
            return {
                'transactions': trans_range,
                'onboarding': onboard_range
            }
            
        except Exception as e:
            st.warning(f"⚠️ Could not get date range: {str(e)}")
            return None
    
    def load_data_from_mysql(self, start_date, end_date, force_reload=False):
        """Load data from MySQL database"""
        # Create cache key
        cache_key = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        
        # Check cache if not forcing reload
        if not force_reload and cache_key in self.data_cache:
            cache_data = self.data_cache[cache_key]
            if time.time() - cache_data['timestamp'] < 300:  # 5 minute cache
                self.transactions = cache_data['transactions']
                self.onboarding = cache_data['onboarding']
                st.success(f"✅ Using cached data")
                return True
        
        # Show loading message
        with st.spinner(f"📥 Loading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}..."):
            try:
                connection = self.connect_to_mysql()
                if connection is None:
                    return False
                
                # Load transaction data
                transaction_query = """
                    SELECT 
                        user_identifier, 
                        entity_name, 
                        status, 
                        service_name, 
                        product_name, 
                        transaction_type, 
                        amount, 
                        ucp_name, 
                        created_at
                    FROM Transaction
                    WHERE DATE(created_at) BETWEEN %s AND %s
                    AND status = 'SUCCESS'
                """
                
                with connection.cursor() as cursor:
                    cursor.execute(transaction_query, (start_date.date(), end_date.date()))
                    transaction_results = cursor.fetchall()
                    
                    if transaction_results:
                        self.transactions = pd.DataFrame(transaction_results)
                    else:
                        self.transactions = pd.DataFrame()
                
                # Load onboarding data
                onboarding_query = """
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
                    FROM Onboarding
                    WHERE DATE(registration_date) BETWEEN %s AND %s
                """
                
                with connection.cursor() as cursor:
                    cursor.execute(onboarding_query, (start_date.date(), end_date.date()))
                    onboarding_results = cursor.fetchall()
                    
                    if onboarding_results:
                        self.onboarding = pd.DataFrame(onboarding_results)
                    else:
                        self.onboarding = pd.DataFrame()
                
                connection.close()
                
                # Cache the data
                if len(self.transactions) > 0 or len(self.onboarding) > 0:
                    self.data_cache[cache_key] = {
                        'transactions': self.transactions.copy(),
                        'onboarding': self.onboarding.copy(),
                        'timestamp': time.time()
                    }
                
                return True
                
            except Exception as e:
                st.error(f"❌ Error loading data: {str(e)}")
                return False
    
    def _preprocess_data(self):
        """Preprocess loaded data"""
        try:
            # Parse dates
            if 'created_at' in self.transactions.columns and len(self.transactions) > 0:
                self.transactions['created_at'] = pd.to_datetime(self.transactions['created_at'], errors='coerce')
            
            if 'registration_date' in self.onboarding.columns and len(self.onboarding) > 0:
                self.onboarding['registration_date'] = pd.to_datetime(self.onboarding['registration_date'], errors='coerce')
            
            # Clean numeric columns
            if 'amount' in self.transactions.columns and len(self.transactions) > 0:
                self.transactions['amount'] = pd.to_numeric(self.transactions['amount'], errors='coerce')
            
            # Create consistent user identifier
            if len(self.transactions) > 0:
                if 'user_identifier' in self.transactions.columns:
                    self.transactions['user_id'] = self.transactions['user_identifier'].astype(str).str.strip()
            
            if len(self.onboarding) > 0:
                if 'mobile' in self.onboarding.columns:
                    self.onboarding['user_id'] = self.onboarding['mobile'].astype(str).str.strip()
            
            return True
            
        except Exception as e:
            st.warning(f"⚠️ Some preprocessing issues: {str(e)}")
            return False
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics"""
        metrics = {}
        
        # New customers from onboarding
        if len(self.onboarding) > 0:
            period_onboarding = self.onboarding[
                (self.onboarding['registration_date'] >= start_date) & 
                (self.onboarding['registration_date'] <= end_date)
            ]
            
            if 'status' in period_onboarding.columns:
                # Segment by status
                status_counts = period_onboarding['status'].value_counts()
                metrics['new_customers_active'] = status_counts.get('Active', 0)
                metrics['new_customers_registered'] = status_counts.get('Registered', 0)
                metrics['new_customers_temporary'] = status_counts.get('TemporaryRegister', 0)
                metrics['new_customers_total'] = len(period_onboarding)
        
        # Active customers from transactions
        if len(self.transactions) > 0:
            period_transactions = self.transactions[
                (self.transactions['created_at'] >= start_date) & 
                (self.transactions['created_at'] <= end_date)
            ]
            
            if len(period_transactions) > 0:
                # Count transactions per user
                user_transaction_counts = period_transactions.groupby('user_id').size()
                
                # Different thresholds for different period types
                if period_type == 'weekly' or period_type == 'rolling':
                    threshold = 2
                else:  # monthly
                    threshold = 10
                
                active_users = user_transaction_counts[user_transaction_counts >= threshold].index.tolist()
                metrics['active_customers_all'] = len(active_users)
            else:
                metrics['active_customers_all'] = 0
        
        # Product performance
        if len(self.transactions) > 0:
            period_transactions = self.transactions[
                (self.transactions['created_at'] >= start_date) & 
                (self.transactions['created_at'] <= end_date)
            ]
            
            if len(period_transactions) > 0 and 'product_name' in period_transactions.columns:
                product_counts = period_transactions['product_name'].value_counts()
                if len(product_counts) > 0:
                    metrics['top_product'] = product_counts.index[0]
                    metrics['top_product_count'] = int(product_counts.iloc[0])
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['top_product_count'] = 0
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date):
        """Calculate Customer Acquisition metrics"""
        metrics = {}
        
        if len(self.onboarding) > 0:
            period_onboarding = self.onboarding[
                (self.onboarding['registration_date'] >= start_date) & 
                (self.onboarding['registration_date'] <= end_date)
            ]
            
            metrics['new_registrations_total'] = len(period_onboarding)
            
            if 'status' in period_onboarding.columns:
                status_counts = period_onboarding['status'].value_counts()
                metrics['new_registrations_active'] = status_counts.get('Active', 0)
                metrics['new_registrations_registered'] = status_counts.get('Registered', 0)
                metrics['new_registrations_temporary'] = status_counts.get('TemporaryRegister', 0)
            
            if 'kyc_status' in period_onboarding.columns:
                kyc_completed = period_onboarding[
                    (period_onboarding['kyc_status'].str.upper() == 'VERIFIED') &
                    (period_onboarding['status'] == 'Active')
                ]
                metrics['kyc_completed'] = len(kyc_completed)
            else:
                metrics['kyc_completed'] = 0
        
        # First-Time Transactors
        if len(self.transactions) > 0 and len(self.onboarding) > 0:
            period_transactions = self.transactions[
                (self.transactions['created_at'] >= start_date) & 
                (self.transactions['created_at'] <= end_date)
            ]
            
            new_customers = set(self.onboarding['user_id'].unique())
            transacting_users = set(period_transactions['user_id'].unique())
            ftt_users = new_customers.intersection(transacting_users)
            metrics['ftt'] = len(ftt_users)
        else:
            metrics['ftt'] = 0
        
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type):
        """Calculate Product Usage Performance metrics"""
        if len(self.transactions) == 0:
            return {}
        
        period_transactions = self.transactions[
            (self.transactions['created_at'] >= start_date) & 
            (self.transactions['created_at'] <= end_date)
        ]
        
        if len(period_transactions) == 0:
            return {}
        
        product_metrics = {}
        
        for product in period_transactions['product_name'].unique():
            if pd.isna(product):
                continue
            
            product_trans = period_transactions[period_transactions['product_name'] == product]
            
            # Active Users
            user_product_counts = product_trans.groupby('user_id').size()
            
            if period_type in ['weekly', 'rolling']:
                threshold = 2
            else:  # monthly
                threshold = 10
            
            active_users_all = (user_product_counts >= threshold).sum()
            
            # Total metrics
            total_transactions = len(product_trans)
            total_amount = product_trans['amount'].sum() if 'amount' in product_trans.columns else 0
            total_users = product_trans['user_id'].nunique()
            avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
            
            # Find category
            category = 'Other'
            for cat, products_list in self.product_categories.items():
                if product in products_list:
                    category = cat
                    break
            
            product_metrics[product] = {
                'category': category,
                'active_users_all': int(active_users_all),
                'total_transactions': int(total_transactions),
                'total_amount': float(total_amount),
                'avg_amount': float(avg_amount),
                'total_users': int(total_users)
            }
        
        return product_metrics
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics"""
        if len(self.transactions) == 0:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0.0,
                'avg_products_per_user': 0.0,
                'total_transactions': 0
            }
        
        period_transactions = self.transactions[
            (self.transactions['created_at'] >= start_date) & 
            (self.transactions['created_at'] <= end_date)
        ]
        
        if len(period_transactions) == 0:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0.0,
                'avg_products_per_user': 0.0,
                'total_transactions': 0
            }
        
        # Active customers
        user_transaction_counts = period_transactions.groupby('user_id').size()
        
        if period_type in ['weekly', 'rolling']:
            threshold = 2
        else:  # monthly
            threshold = 10
        
        active_users = user_transaction_counts[user_transaction_counts >= threshold]
        
        metrics = {
            'wau': int(len(active_users)),
            'avg_transactions_per_user': float(active_users.mean()) if len(active_users) > 0 else 0.0,
            'total_transactions': int(len(period_transactions))
        }
        
        # Average products per user
        if len(active_users) > 0:
            active_user_ids = active_users.index.tolist()
            active_user_transactions = period_transactions[period_transactions['user_id'].isin(active_user_ids)]
            products_per_user = active_user_transactions.groupby('user_id')['product_name'].nunique()
            metrics['avg_products_per_user'] = float(products_per_user.mean()) if len(products_per_user) > 0 else 0.0
        else:
            metrics['avg_products_per_user'] = 0.0
        
        return metrics

def main():
    """Main Streamlit application"""
    # Header
    st.markdown("<h1 class='main-header'>📊 Business Development Performance Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Initialize session state
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    
    # Sidebar - Filters
    with st.sidebar:
        st.markdown("### ⚡ Date Range Selection")
        
        # Get database date range for reference
        generator = PerformanceReportGenerator()
        date_range_info = generator.get_database_date_range()
        
        if date_range_info:
            trans_min = date_range_info['transactions']['min_date']
            trans_max = date_range_info['transactions']['max_date']
            st.info(f"**Database Range:** {trans_min.strftime('%Y-%m-%d')} to {trans_max.strftime('%Y-%m-%d')}")
        
        # Date selection method
        date_option = st.radio(
            "Select date selection method:",
            ["Preset Ranges", "Custom Range"],
            index=0,
            key="date_option_main"
        )
        
        if date_option == "Preset Ranges":
            preset_options = {
                'Last 7 Days': (datetime.now() - timedelta(days=6), datetime.now()),
                'Last 30 Days': (datetime.now() - timedelta(days=29), datetime.now()),
                'This Month': (datetime.now().replace(day=1), datetime.now()),
                'Last Month': (
                    (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1),
                    datetime.now().replace(day=1) - timedelta(days=1)
                ),
                'November 2025': (datetime(2025, 11, 1), datetime(2025, 11, 30)),
                'October 2025': (datetime(2025, 10, 1), datetime(2025, 10, 31)),
                'Full Available Range': (trans_min, trans_max) if date_range_info else (datetime(2025, 10, 1), datetime.now())
            }
            
            selected_preset = st.selectbox(
                "Select Preset Range:",
                list(preset_options.keys()),
                key="preset_selector"
            )
            
            start_date, end_date = preset_options[selected_preset]
            
            st.success(f"✅ Selected: {selected_preset}")
            st.caption(f"Dates: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
        else:  # Custom Range
            st.markdown("#### 📅 Custom Date Range")
            
            # Default to database range or sensible defaults
            if date_range_info:
                default_start = date_range_info['transactions']['min_date']
                default_end = date_range_info['transactions']['max_date']
            else:
                default_start = datetime(2025, 10, 1)
                default_end = datetime.now()
            
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date", 
                    value=default_start,
                    min_value=datetime(2020, 1, 1).date(),
                    max_value=datetime.now().date(),
                    key="custom_start"
                )
            with col2:
                end_date = st.date_input(
                    "End Date", 
                    value=default_end,
                    min_value=datetime(2020, 1, 1).date(),
                    max_value=datetime.now().date(),
                    key="custom_end"
                )
            
            # Convert to datetime
            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = datetime.combine(end_date, datetime.max.time())
            
            # Validate date range
            if start_date > end_date:
                st.error("❌ Start date must be before end date")
                start_date, end_date = end_date, start_date
            
            days_diff = (end_date - start_date).days + 1
            st.success(f"✅ Custom Range: {days_diff} days")
        
        # Period type
        st.markdown("---")
        st.markdown("#### ⏰ Analysis Period Type")
        period_type = st.selectbox(
            "Select period type for analysis:",
            ['Weekly', 'Monthly', '7-Day Rolling'],
            index=0,
            key="period_type_main"
        ).lower()
        
        # Load button
        st.markdown("---")
        load_button = st.button("🚀 Load Data", type="primary", use_container_width=True)
    
    # Main content area
    if load_button or st.session_state.data_loaded:
        # Initialize generator
        generator = PerformanceReportGenerator()
        
        # Load data
        success = generator.load_data_from_mysql(start_date, end_date, force_reload=load_button)
        
        if success:
            st.session_state.data_loaded = True
            
            # Preprocess data
            generator._preprocess_data()
            
            # Display summary
            st.markdown(f"### 📅 Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            col1, col2 = st.columns(2)
            with col1:
                if len(generator.transactions) > 0:
                    st.metric("Transactions", f"{len(generator.transactions):,}")
                    if 'amount' in generator.transactions.columns:
                        total_amount = generator.transactions['amount'].sum()
                        st.metric("Total Amount", f"GMD {total_amount:,.2f}")
                else:
                    st.warning("⚠️ No transaction data")
            
            with col2:
                if len(generator.onboarding) > 0:
                    st.metric("Onboarding Records", f"{len(generator.onboarding):,}")
                    if 'status' in generator.onboarding.columns:
                        active_users = (generator.onboarding['status'] == 'Active').sum()
                        st.metric("Active Users", f"{active_users:,}")
                else:
                    st.warning("⚠️ No onboarding data")
            
            # Calculate and display metrics in tabs
            if len(generator.transactions) > 0 or len(generator.onboarding) > 0:
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "📈 Executive Snapshot", 
                    "👥 Customer Acquisition", 
                    "📊 Product Usage", 
                    "📱 Customer Activity",
                    "📥 Export Data"
                ])
                
                with tab1:
                    st.markdown("<h3 class='sub-header'>📈 Executive Snapshot</h3>", unsafe_allow_html=True)
                    exec_metrics = generator.calculate_executive_snapshot(start_date, end_date, period_type)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("New Customers", exec_metrics.get('new_customers_total', 0))
                    with col2:
                        st.metric("Active Customers", exec_metrics.get('active_customers_all', 0))
                    with col3:
                        st.metric("Top Product", exec_metrics.get('top_product', 'N/A'))
                    with col4:
                        st.metric("Product Usage", exec_metrics.get('top_product_count', 0))
                
                with tab2:
                    st.markdown("<h3 class='sub-header'>👥 Customer Acquisition</h3>", unsafe_allow_html=True)
                    acq_metrics = generator.calculate_customer_acquisition(start_date, end_date)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("New Registrations", acq_metrics.get('new_registrations_total', 0))
                    with col2:
                        st.metric("KYC Completed", acq_metrics.get('kyc_completed', 0))
                    with col3:
                        st.metric("First-Time Transactors", acq_metrics.get('ftt', 0))
                    with col4:
                        total_reg = acq_metrics.get('new_registrations_total', 1)
                        ftt = acq_metrics.get('ftt', 0)
                        activation_rate = (ftt / total_reg * 100) if total_reg > 0 else 0
                        st.metric("Activation Rate", f"{activation_rate:.1f}%")
                
                with tab3:
                    st.markdown("<h3 class='sub-header'>📊 Product Usage Performance</h3>", unsafe_allow_html=True)
                    product_metrics = generator.calculate_product_usage_performance(start_date, end_date, period_type)
                    
                    if product_metrics:
                        # Create dataframe
                        product_data = []
                        for product, metrics in product_metrics.items():
                            product_data.append({
                                'Product': product,
                                'Category': metrics['category'],
                                'Active Users': metrics['active_users_all'],
                                'Transactions': metrics['total_transactions'],
                                'Total Amount': f"GMD {metrics['total_amount']:,.2f}",
                                'Avg Amount': f"GMD {metrics['avg_amount']:,.2f}",
                                'Unique Users': metrics['total_users']
                            })
                        
                        df = pd.DataFrame(product_data)
                        df = df.sort_values('Transactions', ascending=False)
                        
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No product usage data available")
                
                with tab4:
                    st.markdown("<h3 class='sub-header'>📱 Customer Activity & Engagement</h3>", unsafe_allow_html=True)
                    activity_metrics = generator.calculate_customer_activity_engagement(start_date, end_date, period_type)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Weekly Active Users", activity_metrics.get('wau', 0))
                    with col2:
                        st.metric("Total Transactions", activity_metrics.get('total_transactions', 0))
                    with col3:
                        st.metric("Avg Transactions/User", f"{activity_metrics.get('avg_transactions_per_user', 0):.2f}")
                    with col4:
                        st.metric("Avg Products/User", f"{activity_metrics.get('avg_products_per_user', 0):.2f}")
                
                with tab5:
                    st.markdown("<h3 class='sub-header'>📥 Export Data</h3>", unsafe_allow_html=True)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if len(generator.transactions) > 0:
                            csv = generator.transactions.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Transactions CSV",
                                data=csv,
                                file_name=f"transactions_{start_date.date()}_to_{end_date.date()}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                    
                    with col2:
                        if len(generator.onboarding) > 0:
                            csv = generator.onboarding.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Onboarding CSV",
                                data=csv,
                                file_name=f"onboarding_{start_date.date()}_to_{end_date.date()}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
            else:
                st.warning("No data available for the selected period. Please try different dates.")
        else:
            st.error("Failed to load data. Please check your database connection.")
    else:
        # Welcome message
        st.markdown("""
        ## Welcome to the Business Development Performance Dashboard!
        
        This dashboard provides comprehensive analytics for business development and marketing performance.
        
        ### 🚀 How to Get Started
        
        1. **Select Date Range** in the sidebar
           - Choose from **Preset Ranges** (Last 7 Days, Last 30 Days, etc.)
           - OR select **Custom Range** for any date range
           
        2. **Choose Analysis Period Type**
           - **Weekly**: ≥2 transactions for active status
           - **Monthly**: ≥10 transactions for active status
           - **7-Day Rolling**: Rolling 7-day analysis
           
        3. **Click "Load Data"** to generate reports
        
        ### 📊 Available Reports
        
        - **Executive Snapshot**: Key performance indicators
        - **Customer Acquisition**: Registration and activation metrics
        - **Product Usage**: Detailed product performance analysis
        - **Customer Activity**: User engagement metrics
        - **Data Export**: Download reports and raw data
        
        ### ⚡ Flexible Date Filtering
        
        You can select **ANY date range** you want:
        - Last 7 days, 30 days, or custom range
        - Specific months (October 2025, November 2025)
        - Any date combination within your database range
        - The filter is NOT restricted to specific dates
        
        *Ready to begin? Configure your filters in the sidebar!*
        """)
        
        # Show database info if available
        generator = PerformanceReportGenerator()
        date_range_info = generator.get_database_date_range()
        
        if date_range_info:
            trans_min = date_range_info['transactions']['min_date']
            trans_max = date_range_info['transactions']['max_date']
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Database Range", f"{trans_min.strftime('%b %d')} to {trans_max.strftime('%b %d, %Y')}")
            with col2:
                days_coverage = (trans_max - trans_min).days + 1
                st.metric("Days Covered", f"{days_coverage}")
            with col3:
                st.metric("Date Flexibility", "Full Range")

if __name__ == "__main__":
    main()
