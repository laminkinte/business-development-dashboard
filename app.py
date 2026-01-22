import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import pymysql
from pymysql import cursors
import warnings
from dateutil.relativedelta import relativedelta
import os
from io import BytesIO

warnings.filterwarnings('ignore')

# Set page configuration
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
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #374151;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #1E3A8A;
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
    .stDataFrame {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    .stDateInput > div > div > input {
        border-radius: 8px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        border-radius: 4px 4px 0px 0px;
        padding: 10px 20px;
    }
</style>
""", unsafe_allow_html=True)

class PerformanceDashboard:
    def __init__(self):
        self.today = datetime.now()
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
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
        self.all_products.append('Airtime Topup')
        
    def connect_db(self):
        """Connect to MySQL database"""
        try:
            connection = pymysql.connect(
                host='db4free.net',
                user='lamin_d_kinteh',
                password='Lamin@123',
                database='bdp_report',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            return connection
        except Exception as e:
            st.error(f"Database connection failed: {str(e)}")
            st.info("Please check your database credentials and ensure the database is accessible.")
            return None
    
    def test_connection(self):
        """Test database connection and table access"""
        try:
            conn = self.connect_db()
            if conn:
                with conn.cursor() as cursor:
                    # Test Transaction table
                    cursor.execute("SHOW TABLES LIKE 'Transaction'")
                    transaction_table = cursor.fetchone()
                    
                    cursor.execute("SHOW TABLES LIKE 'Onboarding'")
                    onboarding_table = cursor.fetchone()
                    
                    if transaction_table:
                        cursor.execute("SELECT COUNT(*) as count FROM Transaction LIMIT 1")
                        transaction_count = cursor.fetchone()
                    else:
                        transaction_count = {'count': 0}
                    
                    if onboarding_table:
                        cursor.execute("SELECT COUNT(*) as count FROM Onboarding LIMIT 1")
                        onboarding_count = cursor.fetchone()
                    else:
                        onboarding_count = {'count': 0}
                    
                    conn.close()
                    
                    return {
                        'transaction_table_exists': bool(transaction_table),
                        'onboarding_table_exists': bool(onboarding_table),
                        'transaction_count': transaction_count['count'] if transaction_count else 0,
                        'onboarding_count': onboarding_count['count'] if onboarding_count else 0
                    }
            return None
        except Exception as e:
            st.error(f"Connection test failed: {str(e)}")
            return None
    
    def load_transaction_data(self, start_date, end_date):
        """Load transaction data from MySQL with flexible date range"""
        try:
            conn = self.connect_db()
            if conn:
                with conn.cursor() as cursor:
                    # First, let's check if table exists and has data
                    cursor.execute("SHOW TABLES LIKE 'Transaction'")
                    table_exists = cursor.fetchone()
                    
                    if not table_exists:
                        st.warning("⚠️ Transaction table not found in database!")
                        return pd.DataFrame()
                    
                    # Check if created_at column exists
                    cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'Transaction' 
                    AND COLUMN_NAME = 'created_at'
                    """)
                    created_at_exists = cursor.fetchone()
                    
                    if not created_at_exists:
                        st.warning("⚠️ 'created_at' column not found in Transaction table!")
                        # Try to load without date filter
                        query = """
                        SELECT 
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
                        FROM Transaction
                        LIMIT 1000
                        """
                        cursor.execute(query)
                    else:
                        query = """
                        SELECT 
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
                        FROM Transaction
                        WHERE created_at BETWEEN %s AND %s
                        """
                        cursor.execute(query, (start_date, end_date))
                    
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    
                    # Convert data types
                    if not df.empty:
                        # Parse dates
                        if 'created_at' in df.columns:
                            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
                        
                        # Convert numeric columns
                        numeric_cols = ['amount', 'before_balance', 'after_balance']
                        for col in numeric_cols:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                        # Clean text columns
                        text_columns = ['user_identifier', 'product_name', 'entity_name', 
                                      'transaction_type', 'ucp_name', 'service_name', 
                                      'status', 'sub_transaction_id']
                        for col in text_columns:
                            if col in df.columns:
                                df[col] = df[col].astype(str).str.strip()
                    
                    # Show data preview
                    if df.empty:
                        # Try to get any data from the table to diagnose
                        cursor.execute("SELECT * FROM Transaction LIMIT 5")
                        sample = cursor.fetchall()
                        if sample:
                            st.info(f"Found {len(sample)} sample records but none in date range {start_date} to {end_date}")
                    
                    conn.close()
                    return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading transaction data: {str(e)}")
            # Try to get error details
            try:
                conn = self.connect_db()
                if conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SHOW TABLES")
                        tables = cursor.fetchall()
                        st.info(f"Available tables: {[t['Tables_in_bdp_report'] for t in tables]}")
                        conn.close()
            except:
                pass
            return pd.DataFrame()
    
    def load_onboarding_data(self, start_date, end_date):
        """Load onboarding data from MySQL with flexible date range"""
        try:
            conn = self.connect_db()
            if conn:
                with conn.cursor() as cursor:
                    # Check if table exists
                    cursor.execute("SHOW TABLES LIKE 'Onboarding'")
                    table_exists = cursor.fetchone()
                    
                    if not table_exists:
                        st.warning("⚠️ Onboarding table not found in database!")
                        return pd.DataFrame()
                    
                    # Check if registration_date column exists
                    cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'Onboarding' 
                    AND COLUMN_NAME = 'registration_date'
                    """)
                    reg_date_exists = cursor.fetchone()
                    
                    if not reg_date_exists:
                        st.warning("⚠️ 'registration_date' column not found in Onboarding table!")
                        # Load without date filter
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
                        FROM Onboarding
                        LIMIT 1000
                        """
                        cursor.execute(query)
                    else:
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
                        FROM Onboarding
                        WHERE registration_date BETWEEN %s AND %s
                        """
                        cursor.execute(query, (start_date, end_date))
                    
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    
                    # Convert data types
                    if not df.empty:
                        # Parse dates
                        if 'registration_date' in df.columns:
                            df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
                        if 'updated_at' in df.columns:
                            df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
                        
                        # Clean text columns
                        text_columns = ['mobile', 'status', 'kyc_status', 'entity', 'full_name']
                        for col in text_columns:
                            if col in df.columns:
                                df[col] = df[col].astype(str).str.strip()
                    
                    conn.close()
                    return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading onboarding data: {str(e)}")
            return pd.DataFrame()
    
    def calculate_executive_snapshot(self, transactions_df, onboarding_df, period_type='custom'):
        """Calculate executive snapshot metrics with safe defaults"""
        metrics = {
            'new_customers_active': 0,
            'new_customers_registered': 0,
            'new_customers_temporary': 0,
            'new_customers_total': 0,
            'active_customers_all': 0,
            'total_transactions': 0,
            'total_amount': 0,
            'wau_total': 0,
            'top_product': 'N/A',
            'top_product_count': 0,
            'top_product_amount': 0,
            'top_product_users': 0,
            'low_product': 'N/A',
            'low_product_count': 0,
            'low_product_amount': 0,
            'low_product_users': 0
        }
        
        # New registrations by status (from onboarding)
        if not onboarding_df.empty and 'status' in onboarding_df.columns:
            try:
                # Filter for customer entities
                if 'entity' in onboarding_df.columns:
                    customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
                else:
                    customer_onboarding = onboarding_df
                
                if not customer_onboarding.empty:
                    status_counts = customer_onboarding['status'].value_counts()
                    metrics['new_customers_active'] = int(status_counts.get('Active', 0))
                    metrics['new_customers_registered'] = int(status_counts.get('Registered', 0))
                    metrics['new_customers_temporary'] = int(status_counts.get('TemporaryRegister', 0))
                    metrics['new_customers_total'] = int(len(customer_onboarding))
            except Exception as e:
                st.warning(f"Error calculating onboarding metrics: {e}")
        
        # Active customers (from transactions)
        if not transactions_df.empty and 'entity_name' in transactions_df.columns:
            try:
                # Filter successful customer transactions
                customer_transactions = transactions_df[
                    (transactions_df['entity_name'] == 'Customer') &
                    (transactions_df['status'] == 'SUCCESS')
                ]
                
                if not customer_transactions.empty:
                    # Different thresholds for different period types
                    if period_type in ['weekly', 'rolling', 'custom']:
                        threshold = 2
                    else:  # monthly
                        threshold = 10
                    
                    if 'user_identifier' in customer_transactions.columns:
                        user_counts = customer_transactions['user_identifier'].value_counts()
                        active_users = user_counts[user_counts >= threshold].index.tolist()
                        metrics['active_customers_all'] = len(active_users)
                    
                    # Total transactions
                    metrics['total_transactions'] = len(customer_transactions)
                    
                    # Total amount
                    if 'amount' in customer_transactions.columns:
                        metrics['total_amount'] = float(customer_transactions['amount'].sum())
            except Exception as e:
                st.warning(f"Error calculating transaction metrics: {e}")
        
        # WAU calculation (Weekly Active Users from new customers)
        try:
            if metrics['new_customers_total'] > 0:
                if not onboarding_df.empty and not transactions_df.empty:
                    if 'mobile' in onboarding_df.columns and 'user_identifier' in transactions_df.columns:
                        new_customer_ids = onboarding_df['mobile'].dropna().unique()
                        wau_customers = transactions_df[
                            (transactions_df['user_identifier'].isin(new_customer_ids)) &
                            (transactions_df['status'] == 'SUCCESS')
                        ]['user_identifier'].nunique()
                        metrics['wau_total'] = int(wau_customers)
        except:
            metrics['wau_total'] = 0
        
        # Top and low performing products
        if not transactions_df.empty and 'product_name' in transactions_df.columns:
            try:
                # Filter successful customer transactions
                product_transactions = transactions_df[
                    (transactions_df['entity_name'] == 'Customer') &
                    (transactions_df['status'] == 'SUCCESS') &
                    (transactions_df['product_name'].notna())
                ]
                
                if not product_transactions.empty and 'product_name' in product_transactions.columns:
                    product_counts = product_transactions['product_name'].value_counts()
                    
                    if not product_counts.empty:
                        metrics['top_product'] = str(product_counts.index[0])
                        metrics['top_product_count'] = int(product_counts.iloc[0])
                        
                        # Get product amount and users
                        top_product_transactions = product_transactions[
                            product_transactions['product_name'] == metrics['top_product']
                        ]
                        if 'amount' in top_product_transactions.columns:
                            metrics['top_product_amount'] = float(top_product_transactions['amount'].sum())
                        if 'user_identifier' in top_product_transactions.columns:
                            metrics['top_product_users'] = top_product_transactions['user_identifier'].nunique()
                        
                        # Find lowest performing active product
                        active_products = product_counts[product_counts > 0]
                        if not active_products.empty:
                            metrics['low_product'] = str(active_products.index[-1])
                            metrics['low_product_count'] = int(active_products.iloc[-1])
                            
                            # Get low product amount and users
                            low_product_transactions = product_transactions[
                                product_transactions['product_name'] == metrics['low_product']
                            ]
                            if 'amount' in low_product_transactions.columns:
                                metrics['low_product_amount'] = float(low_product_transactions['amount'].sum())
                            if 'user_identifier' in low_product_transactions.columns:
                                metrics['low_product_users'] = low_product_transactions['user_identifier'].nunique()
            except Exception as e:
                st.warning(f"Error calculating product metrics: {e}")
        
        return metrics
    
    def calculate_customer_acquisition(self, transactions_df, onboarding_df):
        """Calculate customer acquisition metrics with safe defaults"""
        metrics = {
            'new_registrations_active': 0,
            'new_registrations_registered': 0,
            'new_registrations_temporary': 0,
            'new_registrations_total': 0,
            'kyc_completed': 0,
            'ftt': 0,
            'ftt_rate': 0,
            'total_transacting_customers': 0
        }
        
        # Filter for customer entities
        if not onboarding_df.empty:
            try:
                if 'entity' in onboarding_df.columns:
                    customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
                else:
                    customer_onboarding = onboarding_df
            except:
                customer_onboarding = pd.DataFrame()
        else:
            customer_onboarding = pd.DataFrame()
        
        # New registrations by status
        if not customer_onboarding.empty and 'status' in customer_onboarding.columns:
            try:
                status_counts = customer_onboarding['status'].value_counts()
                metrics['new_registrations_active'] = int(status_counts.get('Active', 0))
                metrics['new_registrations_registered'] = int(status_counts.get('Registered', 0))
                metrics['new_registrations_temporary'] = int(status_counts.get('TemporaryRegister', 0))
                metrics['new_registrations_total'] = int(len(customer_onboarding))
            except:
                pass
        
        # KYC Completed
        if not customer_onboarding.empty and 'kyc_status' in customer_onboarding.columns:
            try:
                kyc_completed = customer_onboarding[customer_onboarding['kyc_status'].str.upper() == 'VERIFIED']
                metrics['kyc_completed'] = len(kyc_completed)
            except:
                pass
        
        # First-Time Transactors (FTT)
        if not customer_onboarding.empty and not transactions_df.empty:
            try:
                if 'mobile' in customer_onboarding.columns and 'user_identifier' in transactions_df.columns:
                    new_customer_mobiles = customer_onboarding['mobile'].dropna().unique()
                    
                    successful_transactions = transactions_df[
                        (transactions_df['status'] == 'SUCCESS') &
                        (transactions_df['entity_name'] == 'Customer')
                    ]
                    
                    if len(new_customer_mobiles) > 0 and not successful_transactions.empty:
                        transacting_users = successful_transactions['user_identifier'].unique()
                        ftt = len(set(new_customer_mobiles) & set(transacting_users))
                        metrics['ftt'] = ftt
            except:
                pass
        
        # FTT Rate
        if metrics['new_registrations_total'] > 0:
            metrics['ftt_rate'] = (metrics['ftt'] / metrics['new_registrations_total']) * 100
        
        # Total transacting customers
        if not transactions_df.empty:
            try:
                total_transacting_customers = transactions_df[
                    transactions_df['entity_name'] == 'Customer'
                ]['user_identifier'].nunique()
                metrics['total_transacting_customers'] = total_transacting_customers
            except:
                pass
        
        return metrics
    
    def calculate_product_metrics(self, transactions_df):
        """Calculate product usage metrics"""
        if transactions_df.empty:
            return {}
        
        product_metrics = {}
        
        # Process regular products
        for category, products in self.product_categories.items():
            for product in products:
                try:
                    if product == 'Internal Wallet Transfer':
                        # P2P special handling
                        product_trans = transactions_df[
                            (transactions_df['product_name'] == 'Internal Wallet Transfer') &
                            (transactions_df['entity_name'] == 'Customer') &
                            (transactions_df['status'] == 'SUCCESS') &
                            (transactions_df['transaction_type'] == 'DR')
                        ]
                        
                        # Exclude fee transactions
                        if 'ucp_name' in product_trans.columns:
                            product_trans = product_trans[
                                ~product_trans['ucp_name'].astype(str).str.contains('Fee', case=False, na=False)
                            ]
                    else:
                        product_trans = transactions_df[
                            (transactions_df['product_name'] == product) &
                            (transactions_df['entity_name'] == 'Customer') &
                            (transactions_df['status'] == 'SUCCESS')
                        ]
                    
                    if not product_trans.empty:
                        total_transactions = len(product_trans)
                        total_amount = product_trans['amount'].sum() if 'amount' in product_trans.columns else 0
                        total_users = product_trans['user_identifier'].nunique() if 'user_identifier' in product_trans.columns else 0
                        avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                        
                        # Active users
                        if 'user_identifier' in product_trans.columns:
                            user_counts = product_trans['user_identifier'].value_counts()
                            active_users = len(user_counts[user_counts >= 2])
                        else:
                            active_users = 0
                        
                        product_metrics[product] = {
                            'category': category,
                            'total_transactions': int(total_transactions),
                            'total_amount': float(total_amount),
                            'avg_amount': float(avg_amount),
                            'total_users': int(total_users),
                            'active_users': int(active_users),
                            'avg_transactions_per_user': total_transactions / total_users if total_users > 0 else 0
                        }
                except:
                    # Skip this product if there's an error
                    continue
        
        # Process Airtime Topup (Service)
        for service in self.services:
            try:
                service_trans = transactions_df[
                    (transactions_df['service_name'] == 'Airtime Topup') &
                    (transactions_df['entity_name'] == 'Customer') &
                    (transactions_df['status'] == 'SUCCESS') &
                    (transactions_df['transaction_type'] == 'DR')
                ]
                
                if not service_trans.empty:
                    total_transactions = len(service_trans)
                    total_amount = service_trans['amount'].sum() if 'amount' in service_trans.columns else 0
                    total_users = service_trans['user_identifier'].nunique() if 'user_identifier' in service_trans.columns else 0
                    avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                    
                    # Active users
                    if 'user_identifier' in service_trans.columns:
                        user_counts = service_trans['user_identifier'].value_counts()
                        active_users = len(user_counts[user_counts >= 2])
                    else:
                        active_users = 0
                    
                    product_metrics[service] = {
                        'category': 'Airtime Topup',
                        'total_transactions': int(total_transactions),
                        'total_amount': float(total_amount),
                        'avg_amount': float(avg_amount),
                        'total_users': int(total_users),
                        'active_users': int(active_users),
                        'avg_transactions_per_user': total_transactions / total_users if total_users > 0 else 0
                    }
            except:
                continue
        
        return product_metrics
    
    def create_period_selector(self):
        """Create flexible date range selector"""
        st.markdown('<h3 class="sub-header">📅 Select Analysis Period</h3>', 
                   unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            period_type = st.selectbox(
                "Select Period Type",
                ["Custom Range", "Last 7 Days", "Last 30 Days", "Last 90 Days", 
                 "This Month", "Last Month", "This Quarter", "Last Quarter",
                 "Last 6 Months", "Year to Date", "Last Year"],
                index=0
            )
        
        with col2:
            if period_type == "Custom Range":
                start_date = st.date_input("Start Date", value=self.today - timedelta(days=30))
                end_date = st.date_input("End Date", value=self.today)
            elif period_type == "Last 7 Days":
                start_date = self.today - timedelta(days=7)
                end_date = self.today
                st.write(f"**Period:** Last 7 Days")
            elif period_type == "Last 30 Days":
                start_date = self.today - timedelta(days=30)
                end_date = self.today
                st.write(f"**Period:** Last 30 Days")
            elif period_type == "Last 90 Days":
                start_date = self.today - timedelta(days=90)
                end_date = self.today
                st.write(f"**Period:** Last 90 Days")
            elif period_type == "This Month":
                start_date = datetime(self.today.year, self.today.month, 1)
                end_date = self.today
                st.write(f"**Period:** This Month")
            elif period_type == "Last Month":
                if self.today.month == 1:
                    start_date = datetime(self.today.year - 1, 12, 1)
                else:
                    start_date = datetime(self.today.year, self.today.month - 1, 1)
                end_date = start_date + relativedelta(months=1, days=-1)
                st.write(f"**Period:** Last Month")
            elif period_type == "This Quarter":
                quarter = (self.today.month - 1) // 3 + 1
                start_date = datetime(self.today.year, 3 * quarter - 2, 1)
                end_date = self.today
                st.write(f"**Period:** This Quarter (Q{quarter})")
            elif period_type == "Last Quarter":
                quarter = (self.today.month - 1) // 3
                if quarter == 0:
                    start_date = datetime(self.today.year - 1, 10, 1)
                else:
                    start_date = datetime(self.today.year, 3 * quarter - 2, 1)
                end_date = start_date + relativedelta(months=3, days=-1)
                st.write(f"**Period:** Last Quarter (Q{quarter})")
            elif period_type == "Last 6 Months":
                start_date = self.today - timedelta(days=180)
                end_date = self.today
                st.write(f"**Period:** Last 6 Months")
            elif period_type == "Year to Date":
                start_date = datetime(self.today.year, 1, 1)
                end_date = self.today
                st.write(f"**Period:** Year to Date")
            elif period_type == "Last Year":
                start_date = datetime(self.today.year - 1, 1, 1)
                end_date = datetime(self.today.year - 1, 12, 31)
                st.write(f"**Period:** Last Year")
        
        with col3:
            st.markdown("### Date Range Summary")
            st.markdown(f"""
            <div class="metric-card">
                <p><strong>From:</strong> {start_date.strftime('%B %d, %Y')}</p>
                <p><strong>To:</strong> {end_date.strftime('%B %d, %Y')}</p>
                <p><strong>Days Covered:</strong> {(end_date - start_date).days + 1}</p>
            </div>
            """, unsafe_allow_html=True)
        
        return start_date, end_date, period_type
    
    def display_executive_snapshot(self, metrics):
        """Display executive snapshot metrics with safe handling"""
        st.markdown("### 📊 Executive Snapshot")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="New Customers",
                value=f"{metrics.get('new_customers_total', 0):,}",
                delta=f"{metrics.get('new_customers_active', 0):,} Active",
                delta_color="normal"
            )
        
        with col2:
            st.metric(
                label="Active Customers",
                value=f"{metrics.get('active_customers_all', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="Total Transactions",
                value=f"{metrics.get('total_transactions', 0):,}",
                delta=f"${metrics.get('total_amount', 0):,.0f}",
                delta_color="off"
            )
        
        with col4:
            top_product = metrics.get('top_product', 'N/A')
            top_count = metrics.get('top_product_count', 0)
            st.metric(
                label="Top Product",
                value=str(top_product)[:15] + "..." if len(str(top_product)) > 15 else str(top_product),
                delta=f"{top_count:,} txn" if top_product != 'N/A' else "N/A"
            )
        
        # Second row of metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="WAU",
                value=f"{metrics.get('wau_total', 0):,}",
                delta=None
            )
        
        with col2:
            low_product = metrics.get('low_product', 'N/A')
            low_count = metrics.get('low_product_count', 0)
            st.metric(
                label="Lowest Product",
                value=str(low_product)[:15] + "..." if len(str(low_product)) > 15 else str(low_product),
                delta=f"{low_count:,} txn" if low_product != 'N/A' else "N/A"
            )
        
        with col3:
            st.metric(
                label="Top Product Users",
                value=f"{metrics.get('top_product_users', 0):,}",
                delta=f"${metrics.get('top_product_amount', 0):,.0f}",
                delta_color="off"
            )
        
        with col4:
            if metrics.get('low_product') != 'N/A':
                st.metric(
                    label="Low Product Users",
                    value=f"{metrics.get('low_product_users', 0):,}",
                    delta=f"${metrics.get('low_product_amount', 0):,.0f}",
                    delta_color="off"
                )
            else:
                st.metric(
                    label="Low Product Users",
                    value="0",
                    delta="N/A"
                )
        
        # Detailed breakdown
        with st.expander("📋 View Detailed Breakdown", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**New Customers by Status:**")
                st.write(f"• **Active:** {metrics.get('new_customers_active', 0):,}")
                st.write(f"• **Registered:** {metrics.get('new_customers_registered', 0):,}")
                st.write(f"• **Temporary:** {metrics.get('new_customers_temporary', 0):,}")
                st.write(f"• **Total:** {metrics.get('new_customers_total', 0):,}")
            
            with col2:
                st.markdown("**Transaction Summary:**")
                st.write(f"• **Total Transactions:** {metrics.get('total_transactions', 0):,}")
                st.write(f"• **Total Amount:** ${metrics.get('total_amount', 0):,.2f}")
                st.write(f"• **Active Customers:** {metrics.get('active_customers_all', 0):,}")
                st.write(f"• **WAU:** {metrics.get('wau_total', 0):,}")
            
            with col3:
                st.markdown("**Product Performance:**")
                if metrics.get('top_product') != 'N/A':
                    st.write(f"• **Top Product:** {metrics.get('top_product', 'N/A')}")
                    st.write(f"  - Transactions: {metrics.get('top_product_count', 0):,}")
                    st.write(f"  - Users: {metrics.get('top_product_users', 0):,}")
                    st.write(f"  - Amount: ${metrics.get('top_product_amount', 0):,.2f}")
                
                if metrics.get('low_product') != 'N/A':
                    st.write(f"• **Lowest Product:** {metrics.get('low_product', 'N/A')}")
                    st.write(f"  - Transactions: {metrics.get('low_product_count', 0):,}")
                    st.write(f"  - Users: {metrics.get('low_product_users', 0):,}")
                    st.write(f"  - Amount: ${metrics.get('low_product_amount', 0):,.2f}")
    
    def display_customer_acquisition(self, metrics):
        """Display customer acquisition metrics"""
        st.markdown("### 👥 Customer Acquisition")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="New Registrations",
                value=f"{metrics.get('new_registrations_total', 0):,}",
                delta=f"{metrics.get('new_registrations_active', 0):,} Active"
            )
        
        with col2:
            st.metric(
                label="KYC Completed",
                value=f"{metrics.get('kyc_completed', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="First-Time Transactors",
                value=f"{metrics.get('ftt', 0):,}",
                delta=None
            )
        
        with col4:
            ftt_rate = metrics.get('ftt_rate', 0)
            st.metric(
                label="FTT Rate",
                value=f"{ftt_rate:.1f}%",
                delta=None
            )
        
        # Additional metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Registered",
                value=f"{metrics.get('new_registrations_registered', 0):,}",
                delta=None
            )
        
        with col2:
            st.metric(
                label="Temporary",
                value=f"{metrics.get('new_registrations_temporary', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="Total Transacting",
                value=f"{metrics.get('total_transacting_customers', 0):,}",
                delta=None
            )
        
        with col4:
            if metrics.get('new_registrations_total', 0) > 0:
                kyc_rate = (metrics.get('kyc_completed', 0) / metrics.get('new_registrations_total', 1)) * 100
                st.metric(
                    label="KYC Rate",
                    value=f"{kyc_rate:.1f}%",
                    delta=None
                )
    
    def display_product_analysis(self, product_metrics):
        """Display product analysis"""
        st.markdown("### 📈 Product Performance Analysis")
        
        if not product_metrics:
            st.info("📭 No product transaction data available for the selected period.")
            return
        
        # Convert to DataFrame
        product_data = []
        for product, metrics in product_metrics.items():
            product_data.append({
                'Product': product,
                'Category': metrics.get('category', ''),
                'Transactions': metrics.get('total_transactions', 0),
                'Amount ($)': metrics.get('total_amount', 0),
                'Avg Amount ($)': metrics.get('avg_amount', 0),
                'Total Users': metrics.get('total_users', 0),
                'Active Users': metrics.get('active_users', 0),
                'Avg Txns/User': metrics.get('avg_transactions_per_user', 0)
            })
        
        df = pd.DataFrame(product_data)
        
        # Display in tabs
        tab1, tab2 = st.tabs(["📋 Metrics Table", "📊 Visualizations"])
        
        with tab1:
            if not df.empty:
                # Sort by transactions
                df_sorted = df.sort_values('Transactions', ascending=False)
                
                st.dataframe(
                    df_sorted,
                    column_config={
                        "Product": st.column_config.TextColumn("Product", width="medium"),
                        "Category": st.column_config.TextColumn("Category", width="medium"),
                        "Transactions": st.column_config.NumberColumn("Transactions", format="%,d"),
                        "Amount ($)": st.column_config.NumberColumn("Amount", format="$%,.2f"),
                        "Avg Amount ($)": st.column_config.NumberColumn("Avg Amount", format="$%,.2f"),
                        "Total Users": st.column_config.NumberColumn("Users", format="%,d"),
                        "Active Users": st.column_config.NumberColumn("Active Users", format="%,d"),
                        "Avg Txns/User": st.column_config.NumberColumn("Avg Txns/User", format="%.2f")
                    },
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("No product data to display")
        
        with tab2:
            if not df.empty:
                df_sorted = df.sort_values('Transactions', ascending=False)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Top products by transactions
                    top_products = df_sorted.head(10)
                    if not top_products.empty:
                        fig = px.bar(top_products, 
                                   x='Product', y='Transactions',
                                   title='Top Products by Transactions',
                                   color='Category',
                                   text='Transactions')
                        fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                        fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Products by users
                    if not top_products.empty:
                        fig = px.bar(top_products, 
                                   x='Product', y='Total Users',
                                   title='Top Products by Users',
                                   color='Category',
                                   text='Total Users')
                        fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                        fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                        st.plotly_chart(fig, use_container_width=True)
                
                # Amount by category
                category_amount = df.groupby('Category')['Amount ($)'].sum().reset_index()
                if not category_amount.empty:
                    fig = px.pie(category_amount, values='Amount ($)', names='Category',
                               title='Transaction Amount by Category',
                               hole=0.3)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No visualization data available")
    
    def display_transaction_analysis(self, transactions_df):
        """Display transaction analysis"""
        st.markdown("### 💰 Transaction Analysis")
        
        if transactions_df.empty:
            st.info("📭 No transaction data available for the selected period.")
            return
        
        # Summary metrics
        total_transactions = len(transactions_df)
        
        if 'status' in transactions_df.columns:
            successful_transactions = len(transactions_df[transactions_df['status'] == 'SUCCESS'])
            failed_transactions = total_transactions - successful_transactions
            success_rate = (successful_transactions / total_transactions * 100) if total_transactions > 0 else 0
        else:
            successful_transactions = 0
            failed_transactions = 0
            success_rate = 0
        
        # Amount metrics
        if 'amount' in transactions_df.columns:
            total_amount = transactions_df['amount'].sum()
            avg_amount = transactions_df['amount'].mean() if successful_transactions > 0 else 0
        else:
            total_amount = avg_amount = 0
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transactions", f"{total_transactions:,}")
        
        with col2:
            st.metric("Successful", f"{successful_transactions:,}")
        
        with col3:
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        with col4:
            st.metric("Total Amount", f"${total_amount:,.2f}")
        
        # Show data preview
        with st.expander("🔍 View Transaction Data", expanded=False):
            display_cols = []
            for col in ['created_at', 'user_identifier', 'product_name', 
                       'amount', 'status', 'transaction_type', 'service_name']:
                if col in transactions_df.columns:
                    display_cols.append(col)
            
            if display_cols:
                st.dataframe(
                    transactions_df[display_cols].head(100),
                    column_config={
                        "created_at": st.column_config.DatetimeColumn("Date", format="YYYY-MM-DD HH:mm"),
                        "user_identifier": "User ID",
                        "product_name": "Product",
                        "amount": st.column_config.NumberColumn("Amount", format="$%,.2f"),
                        "status": "Status",
                        "transaction_type": "Type",
                        "service_name": "Service"
                    },
                    hide_index=True,
                    use_container_width=True
                )
                st.caption(f"Showing 100 of {len(transactions_df):,} records")
    
    def display_database_diagnostic(self):
        """Display database diagnostic information"""
        with st.expander("🔍 Database Diagnostic", expanded=False):
            st.markdown("### Database Connection Test")
            
            if st.button("Test Database Connection", type="secondary"):
                with st.spinner("Testing connection..."):
                    result = self.test_connection()
                    
                    if result:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if result['transaction_table_exists']:
                                st.success("✅ Transaction table exists")
                                st.info(f"Total records: {result['transaction_count']:,}")
                            else:
                                st.error("❌ Transaction table NOT found")
                            
                        with col2:
                            if result['onboarding_table_exists']:
                                st.success("✅ Onboarding table exists")
                                st.info(f"Total records: {result['onboarding_count']:,}")
                            else:
                                st.error("❌ Onboarding table NOT found")
                        
                        # Sample queries
                        st.markdown("### Sample Queries")
                        
                        # Try to get sample data
                        conn = self.connect_db()
                        if conn:
                            with conn.cursor() as cursor:
                                # Get sample from Transaction
                                if result['transaction_table_exists']:
                                    cursor.execute("SELECT * FROM Transaction LIMIT 5")
                                    sample_tx = cursor.fetchall()
                                    if sample_tx:
                                        st.markdown("**Transaction sample (first 5 records):**")
                                        st.json(sample_tx)
                                
                                # Get sample from Onboarding
                                if result['onboarding_table_exists']:
                                    cursor.execute("SELECT * FROM Onboarding LIMIT 5")
                                    sample_on = cursor.fetchall()
                                    if sample_on:
                                        st.markdown("**Onboarding sample (first 5 records):**")
                                        st.json(sample_on)
                                
                                # Get column names
                                cursor.execute("SHOW COLUMNS FROM Transaction")
                                tx_columns = cursor.fetchall()
                                st.markdown("**Transaction columns:**")
                                st.write([col['Field'] for col in tx_columns])
                                
                                cursor.execute("SHOW COLUMNS FROM Onboarding")
                                on_columns = cursor.fetchall()
                                st.markdown("**Onboarding columns:**")
                                st.write([col['Field'] for col in on_columns])
                                
                                conn.close()
                    else:
                        st.error("❌ Could not connect to database")
    
    def run_dashboard(self):
        """Main dashboard function"""
        # Header
        st.markdown('<h1 class="main-header">Business Development Performance Dashboard</h1>', 
                   unsafe_allow_html=True)
        st.markdown("*Real-time analytics for customer acquisition and product performance*")
        st.markdown("---")
        
        # Database diagnostic
        self.display_database_diagnostic()
        
        # Date range selection
        start_date, end_date, period_type = self.create_period_selector()
        
        st.markdown("---")
        
        # Load data with progress indicator
        with st.spinner(f"Loading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}..."):
            transactions_df = self.load_transaction_data(start_date, end_date)
            onboarding_df = self.load_onboarding_data(start_date, end_date)
        
        # Data summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Transactions Loaded", f"{len(transactions_df):,}")
        with col2:
            st.metric("Onboarding Records", f"{len(onboarding_df):,}")
        with col3:
            days_covered = (end_date - start_date).days + 1
            st.metric("Period Coverage", f"{days_covered} days")
        
        # Show data preview if available
        if not transactions_df.empty:
            with st.expander("📊 Transaction Data Preview", expanded=False):
                st.dataframe(transactions_df.head(10))
        
        if not onboarding_df.empty:
            with st.expander("📋 Onboarding Data Preview", expanded=False):
                st.dataframe(onboarding_df.head(10))
        
        # Check if data is loaded
        if transactions_df.empty and onboarding_df.empty:
            st.warning("⚠️ No data found for the selected period.")
            st.info("""
            **Possible reasons:**
            1. No data exists in the database for this date range
            2. Database tables might be empty
            3. Date columns might have different names
            4. Database connection issue
            
            **Try:**
            - Select a different date range
            - Check the Database Diagnostic section above
            - Verify your database has data for the selected period
            """)
            return
        
        # Display metrics in tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Executive Overview", 
            "👥 Customer Acquisition", 
            "📈 Product Analysis",
            "💰 Transaction Details"
        ])
        
        with tab1:
            try:
                # Calculate executive metrics
                exec_metrics = self.calculate_executive_snapshot(
                    transactions_df, onboarding_df, period_type
                )
                self.display_executive_snapshot(exec_metrics)
                
                # Quick insights
                with st.expander("💡 Quick Insights", expanded=False):
                    if exec_metrics.get('new_customers_total', 0) > 0:
                        st.success(f"🎯 **Customer Growth:** {exec_metrics['new_customers_total']:,} new customers acquired")
                    
                    if exec_metrics.get('active_customers_all', 0) > 0:
                        if exec_metrics['new_customers_total'] > 0:
                            activity_rate = (exec_metrics['active_customers_all'] / exec_metrics['new_customers_total']) * 100
                            st.info(f"📈 **Activity Rate:** {activity_rate:.1f}% of new customers are active")
                    
                    if exec_metrics.get('top_product') != 'N/A':
                        st.info(f"🏆 **Top Performer:** {exec_metrics['top_product']} leads with {exec_metrics['top_product_count']:,} transactions")
                    
                    if exec_metrics.get('total_transactions', 0) == 0:
                        st.warning("⚠️ **No transactions found** - Check if transaction data exists for this period")
                        
            except Exception as e:
                st.error(f"Error in Executive Overview: {str(e)}")
                st.info("Please check the data format and try again.")
        
        with tab2:
            try:
                # Calculate acquisition metrics
                acquisition_metrics = self.calculate_customer_acquisition(
                    transactions_df, onboarding_df
                )
                self.display_customer_acquisition(acquisition_metrics)
                
                # Acquisition insights
                with st.expander("🎯 Acquisition Insights", expanded=False):
                    if acquisition_metrics['new_registrations_total'] > 0:
                        conversion_rate = (acquisition_metrics['ftt'] / acquisition_metrics['new_registrations_total']) * 100
                        st.metric("Registration to FTT Conversion", f"{conversion_rate:.1f}%")
                        
                        kyc_rate = (acquisition_metrics['kyc_completed'] / acquisition_metrics['new_registrations_total']) * 100
                        st.metric("KYC Completion Rate", f"{kyc_rate:.1f}%")
            except Exception as e:
                st.error(f"Error in Customer Acquisition: {str(e)}")
        
        with tab3:
            try:
                # Calculate product metrics
                product_metrics = self.calculate_product_metrics(transactions_df)
                self.display_product_analysis(product_metrics)
            except Exception as e:
                st.error(f"Error in Product Analysis: {str(e)}")
        
        with tab4:
            try:
                self.display_transaction_analysis(transactions_df)
            except Exception as e:
                st.error(f"Error in Transaction Details: {str(e)}")
        
        # Footer
        st.markdown("---")
        st.markdown(
            """
            <div style='text-align: center; color: #6B7280; font-size: 0.9rem; padding: 2rem;'>
                <p>📊 <strong>Business Development Performance Dashboard</strong> | Version 2.1</p>
                <p>📍 Real-time analytics powered by MySQL | Last updated: {}</p>
                <p>📧 For support: analytics@company.com</p>
            </div>
            """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            unsafe_allow_html=True
        )

def main():
    """Main function to run the dashboard"""
    try:
        # Initialize dashboard
        dashboard = PerformanceDashboard()
        
        # Run dashboard
        dashboard.run_dashboard()
        
    except Exception as e:
        st.error(f"🚨 Dashboard Error: {str(e)}")
        st.info("""
        **Please try these steps:**
        1. Refresh the page
        2. Check your internet connection
        3. Verify database credentials are correct
        4. Contact support if the issue persists
        """)

if __name__ == "__main__":
    main()
