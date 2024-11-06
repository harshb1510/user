import pandas as pd
from supabase import create_client, Client
from datetime import datetime

# Supabase URL and Service Role Key
url = 'https://djtgxkgeaphckljxgcck.supabase.co'
key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRqdGd4a2dlYXBoY2tsanhnY2NrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyMDQyMzUyOSwiZXhwIjoyMDM1OTk5NTI5fQ.VCBfpN8aZGcZu_Yp_e66VDMvuWQgAm6Ki1V1tXNpW1M'

supabase: Client = create_client(url, key)

def clean_field(value):
    """Helper function to clean field values"""
    if pd.isna(value) or value == '' or value == 'NOT AVAILABLE':
        return None
    # Convert scientific notation numbers to regular strings
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    return str(value).strip()

# Read data from Excel
users_df = pd.read_excel('profile_records_combined.xlsx')
users_data = users_df.to_dict(orient='records')

# Initialize counters
total_users = len(users_data)
users_updated = 0
errors = 0

print(f"Starting to process {total_users} users...")

for index, user in enumerate(users_data, 1):
    email = user.get('email_address')
    if email:
        email = str(email).strip()
    
    if not email:
        print("Skipping record: Email is mandatory")
        errors += 1
        continue

    print(f"\nProcessing user {index}/{total_users}: {email}")
    
    try:
        # Get current profile using email
        current_profile = supabase.table('profile')\
            .select('*')\
            .eq('email_address', email)\
            .execute()

        if not hasattr(current_profile, 'data') or not current_profile.data:
            print(f"Profile not found for user: {email}")
            errors += 1
            continue

        # Update phone number
        new_phone = clean_field(user.get('phone_number'))
        if not new_phone:
            print(f"No valid phone number for user: {email}")
            continue

        # Update profile directly using email
        update_response = supabase.table('profile')\
            .update({'phone_number': new_phone})\
            .eq('email_address', email)\
            .execute()

        if hasattr(update_response, 'data'):
            print(f"Updated phone number to {new_phone} for user {email}")
            users_updated += 1
        else:
            print(f"Error updating phone for {email}: {update_response}")
            errors += 1

    except Exception as e:
        print(f"Exception for user {email}: {e}")
        errors += 1

# Print final summary
print("\n=== Final Summary ===")
print(f"Total users processed: {total_users}")
print(f"Users updated: {users_updated}")
print(f"Errors encountered: {errors}")
print("========================") 