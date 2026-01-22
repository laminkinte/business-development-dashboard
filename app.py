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
    .period-selector {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border: 1px solid #E5E7EB;
    }
    .stButton button {
        width: 100%;
    }
    .filter-section {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border: 1px solid #E5E7EB;
    }
    .success-box {
        background-color: #D1FAE5;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #10B981;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #FEE2E2;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #EF4444;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #DBEAFE;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #3B82F6;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #FEF3C7;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #F59E0B;
        margin: 1rem 0;
    }
    .date-info {
        background-color: #F0F9FF;
        padding: 0.5rem;
        border-radius: 6px;
        border-left: 3px solid #0EA5E9;
        margin: 0.5rem 0;
        font-size: 0.9rem;
    }
    .suggestion-box {
        background-color: #FCE7F3;
        padding: 0.8rem;
        border-radius: 6px;
        border-left: 3px solid #EC4899;
        margin: 0.5rem 0;
        font-size: 0.9rem;
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
        # Define dynamic date ranges
        self.today = datetime.now()
        
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
        
        # Store loaded data with timestamps for caching
        self.data_cache = {}
        
        # Store database info
        self.db_info = None
    
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
    
    def get_database_info(self):
        """Get database information including date ranges"""
        try:
            connection = self.connect_to_mysql()
            if connection is None:
                return None
            
            with connection.cursor() as cursor:
                # Get transaction date range and count
                cursor.execute("SELECT COUNT(*) as count, MIN(created_at) as min_date, MAX(created_at) as max_date FROM Transaction")
                trans_info = cursor.fetchone()
                
                # Get onboarding date range and count
                cursor.execute("SELECT COUNT(*) as count, MIN(registration_date) as min_date, MAX(registration_date) as max_date FROM Onboarding")
                onboard_info = cursor.fetchone()
                
                # Get daily transaction counts for the last 30 days
                cursor.execute("""
                    SELECT DATE(created_at) as date, COUNT(*) as count 
                    FROM Transaction 
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                    GROUP BY DATE(created_at) 
                    ORDER BY date DESC
                """)
                daily_counts = cursor.fetchall()
                
                # Get days with most transactions
                cursor.execute("""
                    SELECT DATE(created_at) as date, COUNT(*) as count 
                    FROM Transaction 
                    GROUP BY DATE(created_at) 
                    ORDER BY count DESC 
                    LIMIT 5
                """)
                top_days = cursor.fetchall()
            
            connection.close()
            
            self.db_info = {
                'transactions': trans_info,
                'onboarding': onboard_info,
                'daily_counts': daily_counts,
                'top_days': top_days
            }
            
            return self.db_info
            
        except Exception as e:
            st.warning(f"⚠️ Could not get database info: {str(e)}")
            return None
    
    def suggest_best_dates(self):
        """Suggest the best dates based on actual data"""
        if not self.db_info:
            self.get_database_info()
        
        if not self.db_info:
            return None
        
        trans_min = self.db_info['transactions']['min_date']
        trans_max = self.db_info['transactions']['max_date']
        
        if not trans_min or not trans_max:
            return None
        
        # Suggest last 7 days of available data
        if (trans_max - trans_min).days >= 6:
            end_date = trans_max
            start_date = end_date - timedelta(days=6)
        else:
            start_date = trans_min
            end_date = trans_max
        
        # Check if we have good daily data
        good_days = [day for day in self.db_info['daily_counts'] if day['count'] > 100]
        if good_days and len(good_days) >= 3:
            # Use the most recent good days
            good_days.sort(key=lambda x: x['date'], reverse=True)
            start_date = min([day['date'] for day in good_days[:3]])
            end_date = max([day['date'] for day in good_days[:3]])
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'reason': 'Based on actual transaction data in your database'
        }
    
    def display_database_info(self):
        """Display database information"""
        if not self.db_info:
            self.get_database_info()
        
        if not self.db_info:
            return
        
        trans_info = self.db_info['transactions']
        onboard_info = self.db_info['onboarding']
        daily_counts = self.db_info['daily_counts']
        top_days = self.db_info['top_days']
        
        st.markdown("""
        <div class='info-box'>
        <h4>📊 Database Information</h4>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Transactions", f"{trans_info['count']:,}")
            if trans_info['min_date']:
                st.caption(f"Date Range: {trans_info['min_date'].strftime('%Y-%m-%d')} to {trans_info['max_date'].strftime('%Y-%m-%d')}")
                days_diff = (trans_info['max_date'] - trans_info['min_date']).days + 1
                st.caption(f"Covering {days_diff} days")
            
            if daily_counts:
                recent_total = sum([day['count'] for day in daily_counts[:7]])
                st.metric("Last 7 Days", f"{recent_total:,}")
        
        with col2:
            st.metric("Total Onboarding", f"{onboard_info['count']:,}")
            if onboard_info['min_date']:
                st.caption(f"Date Range: {onboard_info['min_date'].strftime('%Y-%m-%d')} to {onboard_info['max_date'].strftime('%Y-%m-%d')}")
            
            if top_days:
                best_day = top_days[0]
                st.caption(f"Busiest Day: {best_day['date'].strftime('%Y-%m-%d')} ({best_day['count']:,} txn)")
        
        # Show recent days with data
        if daily_counts and len(daily_counts) > 0:
            recent_days = daily_counts[:5]
            days_str = ", ".join([f"{day['date'].strftime('%m/%d')} ({day['count']:,})" for day in recent_days])
            st.caption(f"📅 Recent days with data: {days_str}")
        
        # Suggest best dates
        suggestion = self.suggest_best_dates()
        if suggestion:
            st.markdown(f"""
            <div class='suggestion-box'>
            💡 <b>Suggested Date Range:</b> {suggestion['start_date'].strftime('%Y-%m-%d')} to {suggestion['end_date'].strftime('%Y-%m-%d')}<br>
            <small>{suggestion['reason']}</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    def load_data_from_mysql(self, start_date=None, end_date=None, force_reload=False):
        """Load data from MySQL database with caching"""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=7)
        if end_date is None:
            end_date = datetime.now()
        
        # Create cache key
        cache_key = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        
        # Check cache if not forcing reload
        if not force_reload and cache_key in self.data_cache:
            cache_data = self.data_cache[cache_key]
            if time.time() - cache_data['timestamp'] < 300:  # 5 minute cache
                self.transactions = cache_data['transactions']
                self.onboarding = cache_data['onboarding']
                st.success(f"✅ Using cached data (loaded {int(time.time() - cache_data['timestamp'])} seconds ago)")
                return True
        
        # Show date range being loaded
        days_diff = (end_date - start_date).days + 1
        st.markdown(f"""
        <div class='date-info'>
        📅 <b>Loading Data</b><br>
        Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}<br>
        Duration: {days_diff} day{'s' if days_diff != 1 else ''}
        </div>
        """, unsafe_allow_html=True)
        
        # Display database info first
        self.display_database_info()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            connection = self.connect_to_mysql()
            if connection is None:
                return False
            
            # Load transaction data
            status_text.text("📥 Loading transaction data...")
            progress_bar.progress(20)
            
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
                WHERE created_at BETWEEN %s AND %s
                AND status = 'SUCCESS'
            """
            
            with connection.cursor() as cursor:
                cursor.execute(transaction_query, (start_date, end_date))
                transaction_results = cursor.fetchall()
                
                if transaction_results:
                    self.transactions = pd.DataFrame(transaction_results)
                    st.success(f"✅ Loaded {len(self.transactions):,} transaction records")
                    
                    # Show transaction date range
                    if len(self.transactions) > 0 and 'created_at' in self.transactions.columns:
                        trans_dates = pd.to_datetime(self.transactions['created_at'])
                        actual_min = trans_dates.min().strftime('%Y-%m-%d')
                        actual_max = trans_dates.max().strftime('%Y-%m-%d')
                        st.caption(f"📊 Transactions from {actual_min} to {actual_max}")
                else:
                    self.transactions = pd.DataFrame()
                    
                    # Check if dates are outside database range
                    db_info = self.db_info or self.get_database_info()
                    if db_info:
                        trans_min = db_info['transactions']['min_date']
                        trans_max = db_info['transactions']['max_date']
                        
                        if start_date < trans_min or end_date > trans_max:
                            st.warning(f"⚠️ Selected period outside database range")
                            st.info(f"Database has data from {trans_min.strftime('%Y-%m-%d')} to {trans_max.strftime('%Y-%m-%d')}")
                        else:
                            st.warning(f"⚠️ No transactions found for selected period")
            
            progress_bar.progress(50)
            
            # Load onboarding data
            status_text.text("📥 Loading onboarding data...")
            
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
                WHERE registration_date BETWEEN %s AND %s
            """
            
            with connection.cursor() as cursor:
                cursor.execute(onboarding_query, (start_date, end_date))
                onboarding_results = cursor.fetchall()
                
                if onboarding_results:
                    self.onboarding = pd.DataFrame(onboarding_results)
                    st.success(f"✅ Loaded {len(self.onboarding):,} onboarding records")
                else:
                    self.onboarding = pd.DataFrame()
                    st.warning("⚠️ No onboarding records found for selected period")
            
            progress_bar.progress(80)
            
            # Clean and preprocess data
            status_text.text("🧹 Preprocessing data...")
            self._preprocess_data()
            
            progress_bar.progress(100)
            status_text.text("✅ Data loading complete!")
            
            connection.close()
            
            # Cache the data if we have any
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
            
            # Create consistent user identifier for merging
            if len(self.transactions) > 0:
                if 'user_identifier' in self.transactions.columns:
                    self.transactions['user_id'] = self.transactions['user_identifier'].astype(str).str.strip()
                else:
                    self.transactions['user_id'] = 'unknown'
            
            if len(self.onboarding) > 0:
                if 'mobile' in self.onboarding.columns:
                    self.onboarding['user_id'] = self.onboarding['mobile'].astype(str).str.strip()
                else:
                    self.onboarding['user_id'] = 'unknown'
            
            # Clean text columns
            text_columns = ['user_id', 'product_name', 'entity_name', 'transaction_type', 
                           'ucp_name', 'service_name', 'status']
            for col in text_columns:
                if col in self.transactions.columns and len(self.transactions) > 0:
                    self.transactions[col] = self.transactions[col].astype(str).str.strip()
            
            # Clean onboarding text columns
            onboard_text_cols = ['user_id', 'kyc_status', 'status', 'entity']
            for col in onboard_text_cols:
                if col in self.onboarding.columns and len(self.onboarding) > 0:
                    self.onboarding[col] = self.onboarding[col].astype(str).str.strip()
            
            st.success("✅ Data preprocessing complete")
            
        except Exception as e:
            st.warning(f"⚠️ Some preprocessing issues: {str(e)}")
    
    def _display_data_summary(self):
        """Display data summary"""
        with st.expander("📊 Data Summary", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                if len(self.transactions) > 0:
                    st.metric("Transactions", f"{len(self.transactions):,}")
                    if 'amount' in self.transactions.columns:
                        total_amount = self.transactions['amount'].sum()
                        st.metric("Total Amount", f"GMD {total_amount:,.2f}")
                    if 'status' in self.transactions.columns:
                        success_count = (self.transactions['status'] == 'SUCCESS').sum()
                        success_rate = (success_count / len(self.transactions) * 100) if len(self.transactions) > 0 else 0
                        st.metric("Success Rate", f"{success_rate:.1f}%")
                    
                    # Show transaction date range
                    if 'created_at' in self.transactions.columns:
                        min_date = self.transactions['created_at'].min()
                        max_date = self.transactions['created_at'].max()
                        st.caption(f"Transactions from {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                else:
                    st.warning("⚠️ No transaction data")
            
            with col2:
                if len(self.onboarding) > 0:
                    st.metric("Onboarding Records", f"{len(self.onboarding):,}")
                    if 'status' in self.onboarding.columns:
                        active_users = (self.onboarding['status'] == 'Active').sum()
                        st.metric("Active Users", f"{active_users:,}")
                    if 'kyc_status' in self.onboarding.columns:
                        verified_users = (self.onboarding['kyc_status'].str.upper() == 'VERIFIED').sum()
                        st.metric("KYC Verified", f"{verified_users:,}")
                    
                    # Show onboarding date range
                    if 'registration_date' in self.onboarding.columns:
                        min_date = self.onboarding['registration_date'].min()
                        max_date = self.onboarding['registration_date'].max()
                        st.caption(f"Registrations from {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                else:
                    st.warning("⚠️ No onboarding data")
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range"""
        if df.empty or date_col not in df.columns:
            return pd.DataFrame()
        
        valid_dates = df[date_col].notna()
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date) & valid_dates
        return df[mask].copy()
    
    def get_new_registered_customers_segmented(self, start_date, end_date):
        """Get new registered customers segmented by Status"""
        if self.onboarding.empty:
            return {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}, {'Active': [], 'Registered': [], 'TemporaryRegister': [], 'Total': []}
        
        period_onboarding = self.filter_by_date_range(
            self.onboarding, 'registration_date', start_date, end_date
        )
        
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
                segmented_counts[status] = status_customers['user_id'].nunique()
                customer_lists[status] = status_customers['user_id'].unique().tolist()
            
            # Total
            segmented_counts['Total'] = valid_customers['user_id'].nunique()
            customer_lists['Total'] = valid_customers['user_id'].unique().tolist()
        
        return segmented_counts, customer_lists
    
    def get_active_customers_all(self, start_date, end_date, period_type):
        """Get ALL active customers based on period type"""
        if self.transactions.empty:
            return [], 0
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if not period_transactions.empty:
            # Count transactions per user
            user_transaction_counts = period_transactions.groupby('user_id').size()
            
            # Different thresholds for different period types
            if period_type == 'weekly' or period_type == 'rolling':
                threshold = 2
            else:  # monthly
                threshold = 10
            
            active_users = user_transaction_counts[user_transaction_counts >= threshold].index.tolist()
            
            return active_users, len(active_users)
        
        return [], 0
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics"""
        metrics = {}
        
        # Get new registered customers SEGMENTED BY STATUS
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        metrics['new_customers_active'] = segmented_counts['Active']
        metrics['new_customers_registered'] = segmented_counts['Registered']
        metrics['new_customers_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_customers_total'] = segmented_counts['Total']
        
        # Get ALL active customers
        active_customers_all, active_count_all = self.get_active_customers_all(start_date, end_date, period_type)
        metrics['active_customers_all'] = active_count_all
        
        # Weekly Active Users (WAU) from new registered customers
        wau_by_status = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        
        for status in ['Active', 'Registered', 'TemporaryRegister']:
            status_customers = segmented_lists[status]
            if status_customers and not self.transactions.empty:
                # Get transactions for status customers
                period_transactions = self.filter_by_date_range(
                    self.transactions, 'created_at', start_date, end_date
                )
                
                if not period_transactions.empty:
                    # Filter to status customers' successful transactions
                    status_customer_transactions = period_transactions[
                        period_transactions['user_id'].isin(status_customers)
                    ]
                    
                    if not status_customer_transactions.empty:
                        # Count transactions per status customer
                        status_customer_counts = status_customer_transactions.groupby('user_id').size()
                        
                        # Different thresholds for different period types
                        if period_type in ['weekly', 'rolling']:
                            threshold = 2
                        else:  # monthly
                            threshold = 10
                        
                        active_status_customers = status_customer_counts[status_customer_counts >= threshold].index.tolist()
                        wau_by_status[status] = len(active_status_customers)
        
        metrics['wau_active'] = wau_by_status['Active']
        metrics['wau_registered'] = wau_by_status['Registered']
        metrics['wau_temporary'] = wau_by_status['TemporaryRegister']
        metrics['wau_total'] = sum(wau_by_status.values())
        
        # Top and Lowest Performing Products
        if not self.transactions.empty:
            period_transactions = self.filter_by_date_range(
                self.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty and 'product_name' in period_transactions.columns:
                product_counts_dict = {}
                product_users_dict = {}
                product_amount_dict = {}
                
                # Process each product
                for product in period_transactions['product_name'].unique():
                    if pd.isna(product):
                        continue
                    
                    if product == 'Internal Wallet Transfer':
                        # CORRECTED P2P COUNTING
                        p2p_transactions = period_transactions[
                            (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                            (period_transactions['transaction_type'] == 'DR')
                        ]
                        
                        # Exclude fee transactions
                        if 'ucp_name' in p2p_transactions.columns:
                            p2p_transactions = p2p_transactions[
                                ~p2p_transactions['ucp_name'].str.contains('Fee', case=False, na=False)
                            ]
                        
                        product_counts_dict[product] = len(p2p_transactions)
                        product_users_dict[product] = p2p_transactions['user_id'].nunique()
                        product_amount_dict[product] = p2p_transactions['amount'].sum() if 'amount' in p2p_transactions.columns else 0
                    else:
                        # For other products
                        product_transactions = period_transactions[
                            period_transactions['product_name'] == product
                        ]
                        product_counts_dict[product] = len(product_transactions)
                        product_users_dict[product] = product_transactions['user_id'].nunique()
                        product_amount_dict[product] = product_transactions['amount'].sum() if 'amount' in product_transactions.columns else 0
                
                # Also include Airtime Topup as a service
                if 'Airtime Topup' in self.services:
                    airtime_transactions = period_transactions[
                        (period_transactions['service_name'] == 'Airtime Topup') &
                        (period_transactions['transaction_type'] == 'DR')
                    ]
                    product_counts_dict['Airtime Topup'] = len(airtime_transactions)
                    product_users_dict['Airtime Topup'] = airtime_transactions['user_id'].nunique()
                    product_amount_dict['Airtime Topup'] = airtime_transactions['amount'].sum() if 'amount' in airtime_transactions.columns else 0
                
                # Convert to Series for sorting
                if product_counts_dict:
                    product_counts = pd.Series(product_counts_dict)
                    product_counts = product_counts.sort_values(ascending=False)
                    
                    # Get top performing product
                    if len(product_counts) > 0:
                        top_product = product_counts.index[0]
                        top_product_count = int(product_counts.iloc[0])
                        
                        # Get lowest performing product (with at least 1 transaction)
                        active_products = product_counts[product_counts > 0]
                        if len(active_products) > 0:
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
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['low_product'] = 'N/A'
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date):
        """Calculate Customer Acquisition metrics"""
        metrics = {}
        
        # Get segmented customer counts
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        # New Registrations by Status
        metrics['new_registrations_active'] = segmented_counts['Active']
        metrics['new_registrations_registered'] = segmented_counts['Registered']
        metrics['new_registrations_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_registrations_total'] = segmented_counts['Total']
        
        # KYC Completed (Status = Active and KYC Status = Verified)
        if not self.onboarding.empty:
            period_onboarding = self.filter_by_date_range(
                self.onboarding, 'registration_date', start_date, end_date
            )
            
            if not period_onboarding.empty and 'kyc_status' in period_onboarding.columns:
                kyc_completed = period_onboarding[
                    (period_onboarding['kyc_status'].str.upper() == 'VERIFIED') &
                    (period_onboarding['status'] == 'Active')
                ]['user_id'].nunique()
            else:
                kyc_completed = 0
        else:
            kyc_completed = 0
        metrics['kyc_completed'] = kyc_completed
        
        # First-Time Transactors (FTT) - New registered customers who transacted
        new_customers_total = segmented_lists['Total']
        if new_customers_total and not self.transactions.empty:
            period_transactions = self.filter_by_date_range(
                self.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty:
                # Find new customers who transacted
                transacting_new_customers = period_transactions[
                    period_transactions['user_id'].isin(new_customers_total)
                ]['user_id'].unique()
                
                ftt_count = len(transacting_new_customers)
            else:
                ftt_count = 0
        else:
            ftt_count = 0
        metrics['ftt'] = ftt_count
        
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type):
        """Calculate Product Usage Performance metrics"""
        if self.transactions.empty:
            return {}
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if period_transactions.empty:
            return {}
        
        product_metrics = {}
        
        # Process regular products
        for category, products in self.product_categories.items():
            for product in products:
                if product == 'Internal Wallet Transfer':
                    # CORRECTED P2P COUNTING
                    product_trans = period_transactions[
                        (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                        (period_transactions['transaction_type'] == 'DR')
                    ]
                    
                    # Exclude fee transactions
                    if 'ucp_name' in product_trans.columns:
                        product_trans = product_trans[
                            ~product_trans['ucp_name'].str.contains('Fee', case=False, na=False)
                        ]
                else:
                    # For other products
                    product_trans = period_transactions[
                        period_transactions['product_name'] == product
                    ]
                
                if not product_trans.empty:
                    # Active Users
                    user_product_counts = product_trans.groupby('user_id').size()
                    
                    # Different thresholds for different period types
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
                    
                    product_metrics[product] = {
                        'category': category,
                        'active_users_all': int(active_users_all),
                        'total_transactions': int(total_transactions),
                        'total_amount': float(total_amount),
                        'avg_amount': float(avg_amount),
                        'total_users': int(total_users)
                    }
        
        # Process Airtime Topup
        for service in self.services:
            service_trans = period_transactions[
                (period_transactions['service_name'] == service) &
                (period_transactions['transaction_type'] == 'DR')
            ]
            
            if not service_trans.empty:
                user_service_counts = service_trans.groupby('user_id').size()
                
                if period_type in ['weekly', 'rolling']:
                    threshold = 2
                else:  # monthly
                    threshold = 10
                    
                active_users_all = (user_service_counts >= threshold).sum()
                
                total_transactions = len(service_trans)
                total_amount = service_trans['amount'].sum() if 'amount' in service_trans.columns else 0
                total_users = service_trans['user_id'].nunique()
                avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                
                product_metrics[service] = {
                    'category': 'Airtime Topup',
                    'active_users_all': int(active_users_all),
                    'total_transactions': int(total_transactions),
                    'total_amount': float(total_amount),
                    'avg_amount': float(avg_amount),
                    'total_users': int(total_users)
                }
        
        return product_metrics
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics"""
        if self.transactions.empty:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0.0,
                'avg_products_per_user': 0.0,
                'total_transactions': 0
            }
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if period_transactions.empty:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0.0,
                'avg_products_per_user': 0.0,
                'total_transactions': 0
            }
        
        metrics = {}
        
        # Get active customers
        wau_active, wau_count = self.get_active_customers_all(start_date, end_date, period_type)
        metrics['wau'] = int(wau_count)
        
        if wau_active:
            active_user_transactions = period_transactions[
                period_transactions['user_id'].isin(wau_active)
            ]
            
            if not active_user_transactions.empty:
                trans_per_active_user = active_user_transactions.groupby('user_id').size()
                avg_transactions_per_user = float(trans_per_active_user.mean())
                
                products_per_active_user = active_user_transactions.groupby('user_id')['product_name'].nunique()
                avg_products_per_user = float(products_per_active_user.mean())
            else:
                avg_transactions_per_user = 0.0
                avg_products_per_user = 0.0
        else:
            avg_transactions_per_user = 0.0
            avg_products_per_user = 0.0
        
        metrics.update({
            'avg_transactions_per_user': avg_transactions_per_user,
            'avg_products_per_user': avg_products_per_user,
            'total_transactions': int(len(period_transactions))
        })
        
        return metrics

# Display functions
def display_executive_snapshot(metrics, period_name, start_date, end_date):
    """Display Executive Snapshot metrics"""
    st.markdown(f"<h3 class='sub-header'>📈 Executive Snapshot - {period_name}</h3>", unsafe_allow_html=True)
    
    # Show period info
    days_diff = (end_date - start_date).days + 1
    st.caption(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({days_diff} days)")
    
    # Row 1: Main Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_new = metrics.get('new_customers_total', 0)
        st.metric("New Customers (Total)", total_new)
        if total_new > 0:
            with st.expander("📊 Segmented View"):
                st.metric("Active Status", metrics.get('new_customers_active', 0))
                st.metric("Registered Status", metrics.get('new_customers_registered', 0))
                st.metric("Temporary Status", metrics.get('new_customers_temporary', 0))
    
    with col2:
        active_customers = metrics.get('active_customers_all', 0)
        st.metric("Active Customers", active_customers)
    
    with col3:
        wau_total = metrics.get('wau_total', 0)
        st.metric("WAU (New Customers)", wau_total)
        if wau_total > 0:
            with st.expander("👥 WAU by Status"):
                st.metric("Active WAU", metrics.get('wau_active', 0))
                st.metric("Registered WAU", metrics.get('wau_registered', 0))
                st.metric("Temporary WAU", metrics.get('wau_temporary', 0))
    
    with col4:
        top_product = metrics.get('top_product', 'N/A')
        if top_product != 'N/A':
            st.metric("Top Product", top_product)
            st.metric("Transactions", metrics.get('top_product_count', 0))
        else:
            st.info("No product data available")

def display_customer_acquisition(metrics):
    """Display Customer Acquisition metrics"""
    st.markdown("<h3 class='sub-header'>👥 Customer Acquisition</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_reg = metrics.get('new_registrations_total', 0)
        st.metric("New Registrations", total_reg)
        if total_reg > 0:
            with st.expander("📋 By Status"):
                st.metric("Active", metrics.get('new_registrations_active', 0))
                st.metric("Registered", metrics.get('new_registrations_registered', 0))
                st.metric("Temporary", metrics.get('new_registrations_temporary', 0))
    
    with col2:
        kyc_completed = metrics.get('kyc_completed', 0)
        st.metric("KYC Completed", kyc_completed)
        if total_reg > 0:
            kyc_rate = (kyc_completed / total_reg * 100)
            st.caption(f"📈 KYC Rate: {kyc_rate:.1f}%")
    
    with col3:
        ftt = metrics.get('ftt', 0)
        st.metric("First-Time Transactors", ftt)
        if total_reg > 0:
            ftt_rate = (ftt / total_reg * 100)
            st.caption(f"📈 FTT Rate: {ftt_rate:.1f}%")
    
    with col4:
        # Calculate activation rate if we have data
        if total_reg > 0 and ftt > 0:
            activation_rate = (ftt / total_reg * 100)
            st.metric("Activation Rate", f"{activation_rate:.1f}%")
        else:
            st.metric("Activation Rate", "N/A")

def display_product_usage(product_metrics):
    """Display Product Usage Performance"""
    st.markdown("<h3 class='sub-header'>📊 Product Usage Performance</h3>", unsafe_allow_html=True)
    
    if not product_metrics:
        st.info("📭 No product usage data available for this period.")
        return
    
    # Create dataframe for display
    product_data = []
    for product, metrics in product_metrics.items():
        if metrics['total_transactions'] > 0:  # Only show products with transactions
            product_data.append({
                'Product': str(product),
                'Category': str(metrics['category']),
                'Active Users': int(metrics['active_users_all']),
                'Transactions': int(metrics['total_transactions']),
                'Total Amount': float(metrics['total_amount']),
                'Avg Amount': float(metrics['avg_amount']),
                'Unique Users': int(metrics['total_users'])
            })
    
    if product_data:
        df = pd.DataFrame(product_data)
        df = df.sort_values('Transactions', ascending=False)
        
        # Display top products in a table
        st.dataframe(
            df,
            hide_index=True,
            column_config={
                "Product": st.column_config.TextColumn("Product", width="medium"),
                "Category": st.column_config.TextColumn("Category", width="small"),
                "Active Users": st.column_config.NumberColumn("Active Users", format="%d"),
                "Transactions": st.column_config.NumberColumn("Transactions", format="%d"),
                "Total Amount": st.column_config.NumberColumn("Total Amount", format="GMD %.2f"),
                "Avg Amount": st.column_config.NumberColumn("Avg Amount", format="GMD %.2f"),
                "Unique Users": st.column_config.NumberColumn("Unique Users", format="%d")
            }
        )
        
        # Visualizations
        if len(df) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top products by transactions
                top_products = df.head(10).sort_values('Transactions', ascending=True)
                if len(top_products) > 0:
                    fig = px.bar(
                        top_products,
                        y='Product',
                        x='Transactions',
                        title='Top Products by Transactions',
                        orientation='h',
                        color='Transactions',
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Product categories by active users
                category_data = df.groupby('Category')['Active Users'].sum().reset_index()
                if len(category_data) > 0:
                    fig = px.pie(
                        category_data,
                        values='Active Users',
                        names='Category',
                        title='Active Users by Product Category',
                        hole=0.4
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("📭 No product usage data available for this period.")

def display_customer_activity(metrics):
    """Display Customer Activity metrics"""
    st.markdown("<h3 class='sub-header'>📱 Customer Activity & Engagement</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Weekly Active Users", metrics.get('wau', 0))
    
    with col2:
        st.metric("Total Transactions", metrics.get('total_transactions', 0))
    
    with col3:
        st.metric("Avg Transactions/User", f"{metrics.get('avg_transactions_per_user', 0):.2f}")
    
    with col4:
        st.metric("Avg Products/User", f"{metrics.get('avg_products_per_user', 0):.2f}")

def main():
    """Main Streamlit application"""
    # Header
    st.markdown("<h1 class='main-header'>📊 Business Development Performance Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Initialize session state
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'current_filters' not in st.session_state:
        st.session_state.current_filters = {}
    if 'use_suggested_dates' not in st.session_state:
        st.session_state.use_suggested_dates = False
    
    # Sidebar for filters
    with st.sidebar:
        st.markdown("### ⚡ Quick Filters")
        
        # Quick date suggestions
        generator = PerformanceReportGenerator()
        suggestion = generator.suggest_best_dates()
        
        if suggestion and st.button("💡 Use Suggested Dates", use_container_width=True):
            st.session_state.use_suggested_dates = True
            st.session_state.suggested_start = suggestion['start_date']
            st.session_state.suggested_end = suggestion['end_date']
            st.rerun()
        
        # Date range selection
        st.markdown("#### 📅 Date Range Selection")
        
        # Option 1: Quick selection
        date_option = st.radio(
            "Select date range option:",
            ["Quick Selection", "Custom Range"],
            index=0,
            key="date_option"
        )
        
        if date_option == "Quick Selection":
            # Get database info for smart defaults
            db_info = generator.get_database_info()
            if db_info:
                trans_max = db_info['transactions']['max_date']
                # Create smart options based on actual data
                date_options = {
                    'Last 7 Days (Recommended)': (trans_max - timedelta(days=6), trans_max),
                    'Last 14 Days': (trans_max - timedelta(days=13), trans_max),
                    'Full Available Range': (db_info['transactions']['min_date'], trans_max),
                    'Last Month': (trans_max.replace(day=1) - timedelta(days=1), trans_max),
                }
            else:
                # Fallback options
                date_options = {
                    'Last 7 Days': (datetime.now() - timedelta(days=6), datetime.now()),
                    'Last 14 Days': (datetime.now() - timedelta(days=13), datetime.now()),
                    'Last 30 Days': (datetime.now() - timedelta(days=29), datetime.now()),
                }
            
            selected_period = st.selectbox(
                "Select Period",
                list(date_options.keys()),
                key="period_selector"
            )
            
            start_date, end_date = date_options[selected_period]
            
            # Show selected dates
            st.markdown(f"""
            <div class='date-info'>
            ✅ Selected: <b>{selected_period}</b><br>
            📅 Dates: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}
            </div>
            """, unsafe_allow_html=True)
        
        else:  # Custom Range
            selected_period = "Custom Range"
            
            # Use suggested dates if button was clicked
            if st.session_state.get('use_suggested_dates', False):
                default_start = st.session_state.get('suggested_start', datetime.now() - timedelta(days=7))
                default_end = st.session_state.get('suggested_end', datetime.now())
                st.session_state.use_suggested_dates = False
            else:
                default_start = datetime.now() - timedelta(days=7)
                default_end = datetime.now()
            
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date", 
                    default_start,
                    key="custom_start_date"
                )
            with col2:
                end_date = st.date_input(
                    "End Date", 
                    default_end,
                    key="custom_end_date"
                )
            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = datetime.combine(end_date, datetime.max.time())
            
            # Show date info
            days_diff = (end_date - start_date).days + 1
            st.markdown(f"""
            <div class='date-info'>
            ✅ Custom Range Selected<br>
            📅 {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}<br>
            ⏱️ {days_diff} days
            </div>
            """, unsafe_allow_html=True)
        
        # Period type selection
        st.markdown("#### ⏰ Period Type")
        period_type = st.selectbox(
            "Select Analysis Period Type",
            ['Weekly', 'Monthly', '7-Day Rolling'],
            index=0,
            key="period_type_selector"
        ).lower()
        
        # Show period info
        threshold = '≥2 transactions' if period_type in ['weekly', 'rolling'] else '≥10 transactions'
        st.markdown(f"""
        <div class='info-box' style='font-size: 0.9rem; padding: 0.8rem;'>
        <b>📊 Analysis Settings:</b><br>
        • Period Type: <b>{period_type.title()}</b><br>
        • Active User Threshold: <b>{threshold}</b>
        </div>
        """, unsafe_allow_html=True)
        
        # Action buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            load_button = st.button("🚀 Load Data", type="primary", use_container_width=True)
        
        with col2:
            refresh_button = st.button("🔄 Refresh", use_container_width=True)
        
        # Quick tips
        st.markdown("---")
        with st.expander("💡 Quick Tips"):
            st.markdown("""
            **For best results:**
            1. Click **"Use Suggested Dates"** for optimal range
            2. Select **"Last 7 Days (Recommended)"** in Quick Selection
            3. Use **Weekly** analysis for 7-day periods
            
            **Database has transactions from:**
            - Oct 13, 2025 to Nov 30, 2025
            - Most data in late November
            
            **Try these dates:**
            - Nov 24-30, 2025 (busiest period)
            - Nov 20-30, 2025 (good coverage)
            """)
    
    # Main content
    if load_button or refresh_button or st.session_state.data_loaded:
        # Check if filters changed
        current_filters = {
            'start_date': start_date,
            'end_date': end_date,
            'period_type': period_type,
            'date_option': date_option
        }
        
        # Force reload if refresh button clicked or filters changed
        force_reload = refresh_button or (current_filters != st.session_state.get('current_filters', {}))
        
        # Update session state
        st.session_state.current_filters = current_filters
        
        # Load data
        success = generator.load_data_from_mysql(start_date, end_date, force_reload)
        
        if success:
            st.session_state.data_loaded = True
            
            # Display data summary
            generator._display_data_summary()
            
            # Only calculate metrics if we have data
            if len(generator.onboarding) > 0 or len(generator.transactions) > 0:
                # Calculate metrics
                with st.spinner("Calculating metrics..."):
                    # Executive Snapshot
                    exec_metrics = generator.calculate_executive_snapshot(start_date, end_date, period_type)
                    
                    # Customer Acquisition
                    cust_acq_metrics = generator.calculate_customer_acquisition(start_date, end_date)
                    
                    # Product Usage (only if we have transactions)
                    product_metrics = {}
                    if len(generator.transactions) > 0:
                        product_metrics = generator.calculate_product_usage_performance(start_date, end_date, period_type)
                    
                    # Customer Activity
                    activity_metrics = generator.calculate_customer_activity_engagement(start_date, end_date, period_type)
                
                # Display metrics in tabs
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "📈 Executive Snapshot", 
                    "👥 Customer Acquisition", 
                    "📊 Product Usage", 
                    "📱 Customer Activity",
                    "📥 Export Data"
                ])
                
                with tab1:
                    display_executive_snapshot(exec_metrics, selected_period, start_date, end_date)
                
                with tab2:
                    display_customer_acquisition(cust_acq_metrics)
                
                with tab3:
                    display_product_usage(product_metrics)
                
                with tab4:
                    display_customer_activity(activity_metrics)
                
                with tab5:
                    st.markdown("<h3 class='sub-header'>📥 Export Data</h3>", unsafe_allow_html=True)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if not generator.transactions.empty:
                            csv = generator.transactions.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Transactions CSV",
                                data=csv,
                                file_name=f"transactions_{start_date.date()}_to_{end_date.date()}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        else:
                            st.info("No transaction data to export")
                    
                    with col2:
                        if not generator.onboarding.empty:
                            csv = generator.onboarding.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Onboarding CSV",
                                data=csv,
                                file_name=f"onboarding_{start_date.date()}_to_{end_date.date()}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
            else:
                st.error("❌ No data found for the selected period.")
                st.markdown("""
                <div class='warning-box'>
                <b>💡 Try These Solutions:</b>
                1. Click <b>"Use Suggested Dates"</b> button above
                2. Select <b>"Last 7 Days (Recommended)"</b> in Quick Selection
                3. Try dates between <b>Nov 24-30, 2025</b> (busiest period)
                </div>
                """, unsafe_allow_html=True)
        else:
            st.error("❌ Failed to load data. Please check your connection.")
    else:
        # Welcome message
        st.markdown("""
        ## Welcome to the Business Development Performance Dashboard!
        
        ### 🎯 Your Database Analysis
        
        Based on your database scan:
        - **📊 763,289 transactions** available
        - **📅 Date Range**: Oct 13, 2025 to Nov 30, 2025
        - **👥 61,224 onboarding records**
        - **💪 Most active period**: Late November 2025
        
        ### 🚀 Quick Start Guide
        
        1. **Click "Use Suggested Dates"** in sidebar (recommended)
        2. **OR Select "Last 7 Days (Recommended)"** in Quick Selection
        3. **Choose Analysis Period Type** (Weekly/Monthly)
        4. **Click "Load Data"** to begin
        
        ### 📊 Why Suggested Dates Work Best
        
        Your database shows:
        - Transactions concentrated in **late November 2025**
        - **Nov 1-7, 2025** has little to no transaction data
        - **Nov 24-30, 2025** is the busiest period
        
        *Ready to see your actual data? Click "Use Suggested Dates"!*
        """)
        
        # Quick stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Transactions", "763,289")
        with col2:
            st.metric("Data Coverage", "49 days")
        with col3:
            st.metric("Busiest Day", "Nov 26, 2025")

if __name__ == "__main__":
    main()
