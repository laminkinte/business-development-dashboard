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
            st.session_state.transactions = None
        if 'onboarding' not in st.session_state:
            st.session_state.onboarding = None
        if 'product_performance_history' not in st.session_state:
            st.session_state.product_performance_history = {}
    
    def get_db_connection(self):
        """Establish MySQL database connection"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except MySQLError as e:
            st.error(f"Database connection failed: {e}")
            return None
    
    def load_data_from_db(self, start_date, end_date):
        """Load data from MySQL database"""
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
                    transactions_df['created_at'] = pd.to_datetime(transactions_df['created_at'])
                    # Clean numeric columns
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
                    st.warning("No transaction records found in the selected date range")
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
                    onboarding_df['registration_date'] = pd.to_datetime(onboarding_df['registration_date'])
                    onboarding_df['updated_at'] = pd.to_datetime(onboarding_df['updated_at'], errors='coerce')
                    # Create User Identifier for merging
                    onboarding_df['user_identifier'] = onboarding_df['mobile'].astype(str).str.strip()
                    
                    st.session_state.onboarding = onboarding_df
                    st.success(f"✅ Loaded {len(onboarding_df)} onboarding records")
                else:
                    st.warning("No onboarding records found in the selected date range")
                    st.session_state.onboarding = pd.DataFrame()
            
            connection.close()
            st.session_state.data_loaded = True
            return True
            
        except Exception as e:
            st.error(f"Error loading data: {e}")
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
            "Custom Range": "custom"
        }
        
        selected_option = st.sidebar.selectbox(
            "Select Date Range",
            list(date_options.keys()),
            index=1
        )
        
        today = datetime.now()
        
        if selected_option == "Custom Range":
            col1, col2 = st.sidebar.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=today - timedelta(days=30),
                    max_value=today
                )
            with col2:
                end_date = st.date_input(
                    "End Date",
                    value=today,
                    min_value=start_date,
                    max_value=today
                )
        else:
            days = date_options[selected_option]
            
            if days == "month":
                start_date = today.replace(day=1)
                end_date = today
            elif days == "last_month":
                first_day = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
                last_day = today.replace(day=1) - timedelta(days=1)
                start_date = first_day
                end_date = last_day
            elif days == "quarter":
                current_quarter = (today.month - 1) // 3 + 1
                start_date = datetime(today.year, 3 * current_quarter - 2, 1)
                end_date = today
            elif days == "last_quarter":
                current_quarter = (today.month - 1) // 3 + 1
                last_quarter = current_quarter - 1 if current_quarter > 1 else 4
                year = today.year if current_quarter > 1 else today.year - 1
                start_date = datetime(year, 3 * last_quarter - 2, 1)
                if last_quarter == 4:
                    end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_date = datetime(year, 3 * last_quarter + 1, 1) - timedelta(days=1)
            elif days == "ytd":
                start_date = datetime(today.year, 1, 1)
                end_date = today
            else:
                start_date = today - timedelta(days=days)
                end_date = today
        
        # Convert to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
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
        if format_func:
            display_value = format_func(value)
        else:
            display_value = f"{value:,.0f}" if isinstance(value, (int, float)) else str(value)
        
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
        
        # Filter data for the period
        period_transactions = transactions_df[
            (transactions_df['created_at'] >= start_date) & 
            (transactions_df['created_at'] <= end_date)
        ]
        
        period_onboarding = onboarding_df[
            (onboarding_df['registration_date'] >= start_date) & 
            (onboarding_df['registration_date'] <= end_date)
        ]
        
        # New Customers by Status
        if not period_onboarding.empty and 'status' in period_onboarding.columns:
            status_counts = period_onboarding[period_onboarding['entity'] == 'Customer']['status'].value_counts()
            metrics['new_customers_active'] = status_counts.get('Active', 0)
            metrics['new_customers_registered'] = status_counts.get('Registered', 0)
            metrics['new_customers_temporary'] = status_counts.get('TemporaryRegister', 0)
            metrics['new_customers_total'] = period_onboarding[period_onboarding['entity'] == 'Customer']['account_id'].nunique()
        else:
            metrics['new_customers_active'] = 0
            metrics['new_customers_registered'] = 0
            metrics['new_customers_temporary'] = 0
            metrics['new_customers_total'] = 0
        
        # Active Customers (customers with successful transactions)
        if not period_transactions.empty:
            customer_transactions = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                user_transaction_counts = customer_transactions.groupby('user_identifier').size()
                active_customers = user_transaction_counts[user_transaction_counts >= 2].index.tolist()
                metrics['active_customers'] = len(active_customers)
            else:
                metrics['active_customers'] = 0
        else:
            metrics['active_customers'] = 0
        
        # Transaction Volume and Value
        if not period_transactions.empty:
            successful_transactions = period_transactions[period_transactions['status'] == 'SUCCESS']
            metrics['total_transactions'] = len(successful_transactions)
            metrics['transaction_value'] = successful_transactions['amount'].sum()
        else:
            metrics['total_transactions'] = 0
            metrics['transaction_value'] = 0
        
        # Top Product
        if not period_transactions.empty:
            product_counts = period_transactions[
                (period_transactions['status'] == 'SUCCESS') &
                (period_transactions['entity_name'] == 'Customer')
            ]['product_name'].value_counts()
            
            if not product_counts.empty:
                metrics['top_product'] = product_counts.index[0]
                metrics['top_product_count'] = int(product_counts.iloc[0])
            else:
                metrics['top_product'] = 'N/A'
                metrics['top_product_count'] = 0
        else:
            metrics['top_product'] = 'N/A'
            metrics['top_product_count'] = 0
        
        return metrics
    
    def display_executive_snapshot(self, metrics):
        """Display executive snapshot metrics"""
        st.markdown('<div class="sub-header">📈 Executive Snapshot</div>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(self.create_metric_card(
                "New Customers",
                metrics['new_customers_total']
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(self.create_metric_card(
                "Active Customers",
                metrics['active_customers']
            ), unsafe_allow_html=True)
        
        with col3:
            st.markdown(self.create_metric_card(
                "Total Transactions",
                metrics['total_transactions']
            ), unsafe_allow_html=True)
        
        with col4:
            st.markdown(self.create_metric_card(
                "Transaction Value",
                metrics['transaction_value'],
                format_func=lambda x: f"₦{x:,.0f}"
            ), unsafe_allow_html=True)
        
        # Additional metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(self.create_metric_card(
                "Active Status",
                metrics['new_customers_active']
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(self.create_metric_card(
                "Registered Status",
                metrics['new_customers_registered']
            ), unsafe_allow_html=True)
        
        with col3:
            st.markdown(self.create_metric_card(
                "Temporary Status",
                metrics['new_customers_temporary']
            ), unsafe_allow_html=True)
        
        with col4:
            product_text = f"{metrics['top_product'][:15]}..." if len(str(metrics['top_product'])) > 15 else metrics['top_product']
            st.markdown(self.create_metric_card(
                "Top Product",
                f"{product_text} ({metrics['top_product_count']})"
            ), unsafe_allow_html=True)
    
    def display_product_performance(self, transactions_df, selected_products):
        """Display product performance analysis"""
        st.markdown('<div class="sub-header">📊 Product Performance</div>', unsafe_allow_html=True)
        
        # Filter successful customer transactions
        customer_transactions = transactions_df[
            (transactions_df['entity_name'] == 'Customer') &
            (transactions_df['status'] == 'SUCCESS')
        ]
        
        if customer_transactions.empty:
            st.info("No transaction data available for the selected period")
            return
        
        # Prepare product performance data
        product_data = []
        for product in selected_products:
            if product == 'Airtime Topup':
                product_trans = customer_transactions[
                    (customer_transactions['service_name'] == 'Airtime Topup') &
                    (customer_transactions['transaction_type'] == 'DR')
                ]
            elif product == 'Internal Wallet Transfer':
                product_trans = customer_transactions[
                    (customer_transactions['product_name'] == 'Internal Wallet Transfer') &
                    (customer_transactions['transaction_type'] == 'DR')
                ]
                # Exclude fee transactions
                if 'ucp_name' in product_trans.columns:
                    product_trans = product_trans[
                        ~product_trans['ucp_name'].str.contains('Fee', case=False, na=False)
                    ]
            else:
                product_trans = customer_transactions[
                    customer_transactions['product_name'] == product
                ]
            
            if not product_trans.empty:
                product_data.append({
                    'Product': product,
                    'Transactions': len(product_trans),
                    'Unique Users': product_trans['user_identifier'].nunique(),
                    'Total Amount': product_trans['amount'].sum(),
                    'Avg Amount': product_trans['amount'].mean()
                })
        
        if product_data:
            product_df = pd.DataFrame(product_data)
            
            # Display in columns
            col1, col2 = st.columns([3, 2])
            
            with col1:
                # Transactions by product
                fig = px.bar(
                    product_df.sort_values('Transactions', ascending=False).head(10),
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
            
            with col2:
                # Product summary table
                st.markdown("**Product Performance Summary**")
                summary_df = product_df.sort_values('Transactions', ascending=False)
                summary_df = summary_df[['Product', 'Transactions', 'Unique Users', 'Total Amount']]
                summary_df['Total Amount'] = summary_df['Total Amount'].apply(lambda x: f"₦{x:,.0f}")
                st.dataframe(
                    summary_df,
                    use_container_width=True,
                    hide_index=True
                )
            
            # Amount by product
            st.markdown("---")
            col1, col2 = st.columns([2, 1])
            
            with col1:
                fig2 = px.pie(
                    product_df,
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
                avg_trans_per_user = product_df['Transactions'].sum() / product_df['Unique Users'].sum() if product_df['Unique Users'].sum() > 0 else 0
                st.metric("Avg Transactions per User", f"{avg_trans_per_user:.1f}")
                st.metric("Total Unique Product Users", f"{product_df['Unique Users'].sum():,.0f}")
                st.metric("Overall Transaction Value", f"₦{product_df['Total Amount'].sum():,.0f}")
    
    def display_customer_acquisition(self, onboarding_df):
        """Display customer acquisition metrics"""
        st.markdown('<div class="sub-header">👥 Customer Acquisition</div>', unsafe_allow_html=True)
        
        if onboarding_df.empty:
            st.info("No onboarding data available for the selected period")
            return
        
        # Filter for customers only
        customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
        
        if customer_onboarding.empty:
            st.info("No customer onboarding data available")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Registration trend
            daily_registrations = customer_onboarding.set_index('registration_date').resample('D').size()
            fig = px.line(
                x=daily_registrations.index,
                y=daily_registrations.values,
                title='Daily Customer Registrations',
                labels={'x': 'Date', 'y': 'Registrations'}
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Status distribution
            if 'status' in customer_onboarding.columns:
                status_counts = customer_onboarding['status'].value_counts()
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title='Customer Status Distribution',
                    hole=0.3
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            # KYC Status
            if 'kyc_status' in customer_onboarding.columns:
                kyc_counts = customer_onboarding['kyc_status'].value_counts()
                st.markdown("**KYC Status**")
                for status, count in kyc_counts.items():
                    st.progress(
                        count / len(customer_onboarding) if len(customer_onboarding) > 0 else 0,
                        text=f"{status}: {count} ({count/len(customer_onboarding)*100:.1f}%)" if len(customer_onboarding) > 0 else f"{status}: {count}"
                    )
            
            # Metrics
            st.metric("Total Registrations", f"{len(customer_onboarding):,.0f}")
            if 'kyc_status' in customer_onboarding.columns:
                verified_count = customer_onboarding[customer_onboarding['kyc_status'].str.upper() == 'VERIFIED'].shape[0]
                st.metric("KYC Verified", f"{verified_count:,}")
    
    def display_transaction_analysis(self, transactions_df):
        """Display transaction analysis"""
        st.markdown('<div class="sub-header">💳 Transaction Analysis</div>', unsafe_allow_html=True)
        
        if transactions_df.empty:
            st.info("No transaction data available for the selected period")
            return
        
        # Filter successful transactions
        successful_transactions = transactions_df[transactions_df['status'] == 'SUCCESS']
        
        if successful_transactions.empty:
            st.info("No successful transactions in the selected period")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily transaction volume
            daily_transactions = successful_transactions.set_index('created_at').resample('D').size()
            fig = px.line(
                x=daily_transactions.index,
                y=daily_transactions.values,
                title='Daily Transaction Volume',
                labels={'x': 'Date', 'y': 'Transactions'}
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Transaction value trend
            daily_value = successful_transactions.set_index('created_at').resample('D')['amount'].sum()
            fig = px.line(
                x=daily_value.index,
                y=daily_value.values,
                title='Daily Transaction Value',
                labels={'x': 'Date', 'y': 'Amount (₦)'}
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        # Success rate analysis
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_counts = transactions_df['status'].value_counts()
            success_rate = (status_counts.get('SUCCESS', 0) / len(transactions_df)) * 100 if len(transactions_df) > 0 else 0
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        with col2:
            avg_transaction_value = successful_transactions['amount'].mean()
            st.metric("Avg Transaction Value", f"₦{avg_transaction_value:,.0f}")
        
        with col3:
            peak_day = successful_transactions.set_index('created_at').resample('D').size().idxmax()
            peak_day_count = successful_transactions.set_index('created_at').resample('D').size().max()
            st.metric("Peak Day", f"{peak_day.strftime('%b %d')}: {peak_day_count:,}")
    
    def display_trend_analysis(self, transactions_df, onboarding_df):
        """Display trend analysis"""
        st.markdown('<div class="sub-header">📈 Trend Analysis</div>', unsafe_allow_html=True)
        
        if transactions_df.empty or onboarding_df.empty:
            st.info("Insufficient data for trend analysis")
            return
        
        # Weekly trends
        transactions_df['week'] = transactions_df['created_at'].dt.to_period('W').dt.start_time
        onboarding_df['week'] = onboarding_df['registration_date'].dt.to_period('W').dt.start_time
        
        weekly_transactions = transactions_df[transactions_df['status'] == 'SUCCESS'].groupby('week').agg({
            'amount': 'sum',
            'id': 'count'
        }).rename(columns={'id': 'transaction_count', 'amount': 'transaction_value'})
        
        weekly_registrations = onboarding_df[onboarding_df['entity'] == 'Customer'].groupby('week').size().reset_index(name='registrations')
        
        # Merge data
        trend_df = weekly_transactions.reset_index().merge(
            weekly_registrations,
            on='week',
            how='outer'
        ).fillna(0)
        
        if len(trend_df) > 1:
            fig = go.Figure()
            
            # Add transaction count trace
            fig.add_trace(go.Scatter(
                x=trend_df['week'],
                y=trend_df['transaction_count'],
                name='Transaction Count',
                yaxis='y1',
                line=dict(color='#1E3A8A', width=3)
            ))
            
            # Add transaction value trace
            fig.add_trace(go.Scatter(
                x=trend_df['week'],
                y=trend_df['transaction_value'],
                name='Transaction Value (₦)',
                yaxis='y2',
                line=dict(color='#10B981', width=3, dash='dash')
            ))
            
            # Add registrations trace
            fig.add_trace(go.Bar(
                x=trend_df['week'],
                y=trend_df['registrations'],
                name='New Registrations',
                yaxis='y3',
                marker_color='#F59E0B',
                opacity=0.6
            ))
            
            fig.update_layout(
                title='Weekly Performance Trends',
                xaxis=dict(title='Week'),
                yaxis=dict(
                    title='Transaction Count',
                    titlefont=dict(color='#1E3A8A'),
                    tickfont=dict(color='#1E3A8A')
                ),
                yaxis2=dict(
                    title='Transaction Value (₦)',
                    titlefont=dict(color='#10B981'),
                    tickfont=dict(color='#10B981'),
                    anchor='x',
                    overlaying='y',
                    side='right'
                ),
                yaxis3=dict(
                    title='New Registrations',
                    titlefont=dict(color='#F59E0B'),
                    tickfont=dict(color='#F59E0B'),
                    anchor='free',
                    overlaying='y',
                    side='right',
                    position=0.95
                ),
                height=500,
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data points for trend analysis")
    
    def run_dashboard(self):
        """Main method to run the dashboard"""
        # Header
        st.markdown('<div class="main-header">📊 Business Development Performance Dashboard</div>', unsafe_allow_html=True)
        
        # Sidebar filters
        with st.sidebar:
            st.markdown("### 🎯 Dashboard Filters")
            
            # Date range selection
            start_date, end_date = self.create_date_filters()
            
            # Product filters
            selected_products = self.create_product_filters()
            
            # Load data button
            st.markdown("---")
            if st.button("🚀 Load Data", type="primary", use_container_width=True):
                with st.spinner("Loading data from database..."):
                    success = self.load_data_from_db(start_date, end_date)
                    if success:
                        st.success("Data loaded successfully!")
            
            # Data status
            st.markdown("---")
            st.markdown("### 📊 Data Status")
            if st.session_state.data_loaded:
                if st.session_state.transactions is not None:
                    st.success(f"✅ Transactions: {len(st.session_state.transactions):,} records")
                if st.session_state.onboarding is not None:
                    st.success(f"✅ Onboarding: {len(st.session_state.onboarding):,} records")
            else:
                st.info("👈 Click 'Load Data' to begin analysis")
            
            # Date range display
            st.markdown("---")
            st.markdown(f"**Selected Date Range:**")
            st.markdown(f"{start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}")
            days_diff = (end_date - start_date).days + 1
            st.markdown(f"**Duration:** {days_diff} days")
        
        # Main content area
        if st.session_state.data_loaded and st.session_state.transactions is not None:
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
            
            # Customer Acquisition
            self.display_customer_acquisition(st.session_state.onboarding)
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Transaction Analysis
            self.display_transaction_analysis(st.session_state.transactions)
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Trend Analysis
            self.display_trend_analysis(st.session_state.transactions, st.session_state.onboarding)
            
            # Export options
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("📥 Export Report to CSV", use_container_width=True):
                    # Export transactions
                    csv_transactions = st.session_state.transactions.to_csv(index=False)
                    st.download_button(
                        label="Download Transaction Data",
                        data=csv_transactions,
                        file_name=f"transactions_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            
            with col2:
                if st.button("📥 Export Onboarding Data", use_container_width=True):
                    # Export onboarding
                    csv_onboarding = st.session_state.onboarding.to_csv(index=False)
                    st.download_button(
                        label="Download Onboarding Data",
                        data=csv_onboarding,
                        file_name=f"onboarding_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            
            # Data preview
            with st.expander("🔍 Preview Raw Data"):
                tab1, tab2 = st.tabs(["Transaction Data", "Onboarding Data"])
                
                with tab1:
                    st.dataframe(
                        st.session_state.transactions.head(100),
                        use_container_width=True,
                        hide_index=True
                    )
                
                with tab2:
                    st.dataframe(
                        st.session_state.onboarding.head(100),
                        use_container_width=True,
                        hide_index=True
                    )
        
        else:
            # Welcome/instructions
            st.info("👈 Use the sidebar to select a date range and load data from the database")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### 📈 Key Features")
                st.markdown("""
                - Flexible date range selection
                - Real-time MySQL data loading
                - Product performance analysis
                - Customer acquisition tracking
                - Transaction trend visualization
                - Export capabilities
                """)
            
            with col2:
                st.markdown("### 🎯 Metrics Tracked")
                st.markdown("""
                - New customer registrations
                - Active customer count
                - Transaction volume & value
                - Product usage patterns
                - KYC completion rates
                - Success rates
                """)
            
            with col3:
                st.markdown("### 📊 Visualizations")
                st.markdown("""
                - Interactive charts
                - Trend analysis
                - Product comparisons
                - Performance dashboards
                - Custom date ranges
                - Real-time updates
                """)
            
            # Sample visualization placeholder
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            st.markdown("### 📋 Sample Dashboard Preview")
            
            # Create sample data for preview
            sample_dates = pd.date_range(start=datetime.now() - timedelta(days=30), end=datetime.now(), freq='D')
            sample_data = pd.DataFrame({
                'date': sample_dates,
                'transactions': np.random.randint(100, 500, len(sample_dates)),
                'registrations': np.random.randint(10, 50, len(sample_dates))
            })
            
            fig = px.line(
                sample_data,
                x='date',
                y=['transactions', 'registrations'],
                title='Sample: Daily Transactions & Registrations',
                labels={'value': 'Count', 'date': 'Date'},
                color_discrete_sequence=['#1E3A8A', '#10B981']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

# Main execution
def main():
    # Initialize dashboard
    dashboard = PerformanceDashboard()
    
    # Run dashboard
    dashboard.run_dashboard()

if __name__ == "__main__":
    main()
