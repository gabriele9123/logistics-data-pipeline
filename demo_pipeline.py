"""
Logistics Data Pipeline Demo

Demonstrates the complete ETL pipeline by fetching real-time flight and bike data
and saving it to CSV files for verification.

Usage:
    python demo_pipeline.py
    
Output:
    data/flights_output.csv
    data/bikes_output.csv
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add scripts to path
sys.path.append(str(Path(__file__).parent / 'scripts'))

from extractors.flights_extractor import FlightsExtractor
from extractors.citybikes_extractor import CityBikesExtractor
from transformers.flights_transformer import FlightsTransformer
from transformers.bikes_transformer import BikesTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ensure_data_directory():
    """Create data directory if it doesn't exist"""
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)
    return data_dir


def run_flights_pipeline(data_dir: Path):
    """
    Run flights extraction and transformation pipeline
    
    Args:
        data_dir: Directory to save output
    """
    logger.info("=" * 60)
    logger.info("FLIGHTS PIPELINE")
    logger.info("=" * 60)
    
    # Major airport bounding boxes
    airports = [
        {
            'code': 'JFK',
            'name': 'John F. Kennedy International Airport',
            'bbox': [-74.2, 40.5, -73.7, 40.8]  # New York area
        },
        {
            'code': 'LHR',
            'name': 'London Heathrow Airport',
            'bbox': [-0.7, 51.3, 0.2, 51.7]  # London area
        },
        {
            'code': 'FCO',
            'name': 'Rome Fiumicino Airport',
            'bbox': [12.0, 41.6, 12.8, 42.0]  # Rome area
        }
    ]
    
    # Extract
    logger.info(f"Extracting flight data for {len(airports)} airports...")
    extractor = FlightsExtractor()
    raw_data = extractor.extract_flights_for_airports(airports)
    
    # Transform
    logger.info("Transforming flight data...")
    transformer = FlightsTransformer()
    
    # Log extracted counts
    for airport in airports:
        code = airport['code']
        flights = raw_data.get(code, [])
        if flights:
            logger.info(f"Extracted {len(flights)} flights for {airport['name']}")
    
    # Transform all flights at once
    df = transformer.transform(raw_data)
    
    # Convert to DataFrame and save
    if not df.empty:
        output_file = data_dir / 'flights_output.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("FLIGHTS DATA SAMPLE:")
        logger.info(f"{'='*60}")
        print(df.head(10).to_string())
        logger.info(f"\n✓ Saved {len(df)} flight records to: {output_file}")
        logger.info(f"{'='*60}\n")
    else:
        logger.warning("No transformed flight data to save")


def run_bikes_pipeline(data_dir: Path):
    """
    Run bike sharing extraction and transformation pipeline
    
    Args:
        data_dir: Directory to save output
    """
    logger.info("=" * 60)
    logger.info("BIKE SHARING PIPELINE")
    logger.info("=" * 60)
    
    # Major cities with bike sharing
    networks = ['citi-bike-nyc', 'santander-cycles', 'velib']
    
    # Extract
    logger.info(f"Extracting bike data for networks: {networks}")
    extractor = CityBikesExtractor()
    raw_data = extractor.extract_all_networks(networks)
    
    # Transform
    logger.info("Transforming bike data...")
    transformer = BikesTransformer()
    
    # Transform all networks at once
    df = transformer.transform(raw_data)
    
    # Convert to DataFrame and save
    if not df.empty:
        output_file = data_dir / 'bikes_output.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("BIKE SHARING DATA SAMPLE:")
        logger.info(f"{'='*60}")
        print(df.head(10).to_string())
        logger.info(f"\n✓ Saved {len(df)} station records to: {output_file}")
        logger.info(f"{'='*60}\n")
    else:
        logger.warning("No transformed bike data to save")


def main():
    """Run the complete logistics pipeline demo"""
    print("\n" + "="*60)
    print("LOGISTICS DATA PIPELINE DEMO")
    print("="*60 + "\n")
    
    # Ensure output directory exists
    data_dir = ensure_data_directory()
    logger.info(f"Output directory: {data_dir}\n")
    
    try:
        # Run pipelines
        run_flights_pipeline(data_dir)
        run_bikes_pipeline(data_dir)
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"\nOutput files saved in: {data_dir}")
        print("- flights_output.csv")
        print("- bikes_output.csv")
        print("\n" + "="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
