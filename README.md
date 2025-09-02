# 🏠 Housing Market Data Analysis Pipeline

A comprehensive Python application that aggregates and analyzes housing market data from multiple sources including Zillow-style APIs, creating professional market insights and visualizations.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![AWS Lambda](https://img.shields.io/badge/AWS-Lambda%20Ready-orange.svg)](https://aws.amazon.com/lambda/)

## 📊 What It Does

This application provides real estate professionals, researchers, and analysts with comprehensive housing market intelligence by:

- **Aggregating Multi-Source Data**: Combines data from Zillow research feeds, U.S. Census Bureau, Federal Reserve, and premium real estate APIs
- **Market Trend Analysis**: Performs statistical analysis on price trends, regional shifts, and affordability metrics  
- **Professional Visualizations**: Generates Zillow-style dashboards and publication-ready charts
- **Automated Reporting**: Creates comprehensive market reports with actionable insights
- **Cloud-Ready Deployment**: Designed for AWS Lambda with S3 compatibility

## 🗂️ Project Structure

```
housing-market-analyzer/
├── housing_analyzer.py          # Main application
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── data/                       # Generated CSV files and reports
│   ├── housing_data.csv
│   ├── demographics_data.csv
│   ├── price_trends.csv
│   ├── regional_analysis.csv
│   └── analysis_summary.json
├── visualizations/             # Generated charts and dashboards
│   ├── price_trends.png
│   ├── regional_analysis.png
│   └── zillow_style_dashboard.png
└── housing_data.db            # SQLite database
```

## 📈 Key Features

### Data Sources Integrated
- **🏠 Zillow Research Data** (FREE) - ZHVI and ZRI indices
- **🏛️ U.S. Census Bureau** (FREE) - Demographics and economic indicators
- **📊 Federal Reserve FRED** (FREE) - Housing market indicators and mortgage rates
- **🏘️ RealtyMole API** (Premium) - Property data and comparables
- **🏢 Realty-in-US API** (Premium) - Market listings and inventory data
- **🏠 Rental Market APIs** (Premium) - Rental pricing and availability

### Analytics Capabilities
- **Price Trend Analysis** - Historical and current market trends
- **Regional Comparisons** - State and metro-level market analysis
- **Affordability Metrics** - Price-to-income and rent-vs-buy analysis
- **Market Velocity** - Days on market and inventory analysis
- **Demographic Correlations** - Population and economic factor analysis

### Visualizations
- **Zillow-Style Dashboards** - 6-panel professional market overview
- **Price Trend Charts** - Time series analysis with state comparisons
- **Regional Heat Maps** - Geographic price change visualization
- **Affordability Analysis** - Market accessibility charts
- **Market Velocity Plots** - Supply and demand indicators

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8 or higher
- pip package manager
- Internet connection for API access

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/housing-market-analyzer.git
cd housing-market-analyzer
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. API Key Configuration

#### Free APIs (Recommended)
Get these free API keys to access real data:

**U.S. Census Bureau API** (FREE)
1. Visit: https://api.census.gov/data/key_signup.html
2. Request your free API key
3. Set environment variable:
```bash
export CENSUS_API_KEY="your_census_api_key_here"
```

**Federal Reserve FRED API** (FREE)  
1. Visit: https://fred.stlouisfed.org/docs/api/api_key.html
2. Create free account and get API key
3. Set environment variable:
```bash
export FRED_API_KEY="your_fred_api_key_here"
```

#### Premium APIs (Optional)
For enhanced real estate data:

**RapidAPI** (Premium)
1. Visit: https://rapidapi.com
2. Subscribe to RealtyMole and Realty-in-US APIs
3. Set environment variables:
```bash
export RAPIDAPI_KEY="your_rapidapi_key_here"
export REALTY_API_KEY="your_realty_api_key_here"
export RENTALS_API_KEY="your_rentals_api_key_here"
```

### 4. Environment Setup Options

#### Option A: Export Variables (Linux/Mac)
```bash
export CENSUS_API_KEY="your_key"
export FRED_API_KEY="your_key"
export RAPIDAPI_KEY="your_key"  # Optional
```

#### Option B: Create .env File
```bash
# Create .env file
echo "CENSUS_API_KEY=your_census_key" > .env
echo "FRED_API_KEY=your_fred_key" >> .env
echo "RAPIDAPI_KEY=your_rapidapi_key" >> .env
```

#### Option C: Windows PowerShell
```powershell
$env:CENSUS_API_KEY="your_key"
$env:FRED_API_KEY="your_key"
$env:RAPIDAPI_KEY="your_key"
```

## 🚀 Usage

### Basic Usage (No API Keys Required)
The application works with realistic fallback data if no API keys are provided:

```bash
python housing_analyzer.py
```

### With API Keys (Recommended)
For real market data:

```bash
# Set your API keys first
export CENSUS_API_KEY="your_key"
export FRED_API_KEY="your_key"

# Run the analyzer
python housing_analyzer.py
```

### Expected Output
```
🏠 HOUSING MARKET ANALYZER WITH ZILLOW INTEGRATION
================================================================

🔧 API Status Check: 3/5 APIs accessible
  ✓ census_api: ✓
  ✓ fred_api: ✓
  ✓ zillow_research: ✓
  ✗ realty_mole: ✗
  ✗ rentals_api: ✗

📊 Collecting data from real APIs including Zillow-style sources
📋 Processing and analyzing 180 housing records across 8 states
📈 Generating professional visualizations...

✅ Pipeline completed successfully!

📁 Generated Files:
   • Database: housing_data.db
   • CSV Files: data/*.csv
   • Visualizations: visualizations/*.png
   • Analysis Summary: data/analysis_summary.json

✨ Check the 'visualizations' folder for Zillow-style charts!
```

## 📋 Requirements

```txt
pandas>=1.3.0
numpy>=1.21.0
matplotlib>=3.4.0
seaborn>=0.11.0
requests>=2.25.0
sqlite3 (built-in)
```

## 🔧 Configuration Options

### Custom Markets
Modify the cities list in `DataCollector` class:
```python
cities = [
    {'city': 'Your City', 'state': 'ST'},
    # Add more cities...
]
```

### Analysis Parameters
Adjust trend analysis settings:
```python
# In analyzer.py - modify date ranges, statistical methods
date_range = 12  # months
outlier_threshold = 1.5  # IQR multiplier
```

### Visualization Styling
Customize chart appearance:
```python
# In visualizer.py - modify colors, styles
plt.style.use('your_preferred_style')
color_palette = 'your_palette'
```

## 🌐 AWS Deployment

### Lambda Function Setup
1. **Package Dependencies**:
```bash
pip install -r requirements.txt -t ./lambda-package
cp housing_analyzer.py ./lambda-package/
```

2. **Create Lambda Function**:
```python
def lambda_handler(event, context):
    return main()
```

3. **Environment Variables**: Set API keys in Lambda console

4. **S3 Integration**: Configure S3 bucket for output storage

### CloudFormation Template
```yaml
Resources:
  HousingAnalyzerFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: housing-market-analyzer
      Runtime: python3.9
      Handler: housing_analyzer.lambda_handler
      Environment:
        Variables:
          CENSUS_API_KEY: !Ref CensusApiKey
          FRED_API_KEY: !Ref FredApiKey
```

## 📊 Output Data Schema

### Housing Data CSV
```csv
state,city,county,median_price,median_rent,price_per_sqft,inventory_count,days_on_market,price_change_pct,date_recorded,data_source
CA,Los Angeles,Los Angeles,850000,3200,567,245,45,3.2,2025-01-15,zillow_research
```

### Analysis Summary JSON
```json
{
  "total_records": 180,
  "states_analyzed": 8,
  "cities_analyzed": 15,
  "avg_median_price": 625000.50,
  "data_sources_used": {
    "zillow_research": 45,
    "fred_estimated": 89,
    "census_api": 46
  },
  "zillow_integration": "active"
}
```

## 🤝 Contributing

1. **Fork the Repository**
2. **Create Feature Branch**: `git checkout -b feature/amazing-feature`
3. **Commit Changes**: `git commit -m 'Add amazing feature'`
4. **Push to Branch**: `git push origin feature/amazing-feature`
5. **Open Pull Request**

### Development Guidelines
- Follow PEP 8 style guidelines
- Add unit tests for new features
- Update documentation for API changes
- Test with both API keys and fallback data

## ⚠️ Disclaimer

This tool is for informational and educational purposes only. The housing market data and analysis provided should not be considered as financial or investment advice. Always consult with qualified professionals before making real estate decisions.

## 🆘 Troubleshooting

### Common Issues

**API Rate Limiting**
```
Solution: The application includes automatic rate limiting delays
```

**Missing Dependencies**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**API Key Issues**
```bash
# Verify your keys are set
echo $CENSUS_API_KEY
echo $FRED_API_KEY

# Test API connectivity
python -c "import os; print('Keys configured:', bool(os.getenv('CENSUS_API_KEY')))"
```

**Memory Issues with Large Datasets**
```python
# Reduce data scope in configuration
states = ['CA', 'TX']  # Instead of all states
date_range = 6  # Instead of 12 months
```

## 🏆 Acknowledgments

- **Zillow Research** for public housing data feeds
- **U.S. Census Bureau** for demographic data APIs
- **Federal Reserve** for economic indicator APIs
- **Contributors** who helped improve this project

---

**⭐ If this project helped you, please give it a star!**
