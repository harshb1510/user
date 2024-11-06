import pandas as pd
from supabase import create_client, Client
from datetime import datetime

# Supabase URL and Service Role Key (replace with your actual credentials)
url = 'https://djtgxkgeaphckljxgcck.supabase.co'
key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRqdGd4a2dlYXBoY2tsanhnY2NrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyMDQyMzUyOSwiZXhwIjoyMDM1OTk5NTI5fQ.VCBfpN8aZGcZu_Yp_e66VDMvuWQgAm6Ki1V1tXNpW1M'

supabase: Client = create_client(url, key)

job_id = input("Please enter the Job ID for the interviews: ")
print(f"Using Job ID: {job_id} for all interviews")

# Step 1: Read profile data from CSV
users_df = pd.read_csv('sample_profiles.csv')
users_data = users_df.to_dict(orient='records')

def clean_field(value):
    """Helper function to clean field values"""
    if pd.isna(value) or value == '' or value == 'NOT AVAILABLE':
        return None
    # Convert scientific notation numbers to regular strings
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    return str(value).strip()

# Initialize counters
total_users = len(users_data)
users_updated = 0
users_created = 0
profiles_updated = 0
interviews_created = 0
interviews_existing = 0
errors = 0

print(f"Starting to process {total_users} users...")

# Step 2: Insert users and profiles using Supabase client
for index, user in enumerate(users_data, 1):
    email = user.get('email_address')
    if email:
        email = str(email).strip()
    password = 'defaultpassword'

    if not email:
        print("Skipping record: Email is mandatory")
        errors += 1
        continue

    print(f"\nProcessing user {index}/{total_users}: {email}")
    
    try:
        existing_users = supabase.auth.admin.list_users()
        existing_user = [u for u in existing_users if u.email == email]

        if existing_user:
            uid = existing_user[0].id
            print(f"Updating existing user {email} with UID {uid}")
            users_updated += 1
        else:
            # Create new user
            user_response = supabase.auth.admin.create_user({
                "email": email,
                "password": password,
                "user_metadata": {"name": f"{user.get('first_name')} {user.get('last_name')}"},
                "email_confirm": True
            })
            uid = user_response.user.id
            print(f"Created new user {email} with UID {uid}")
            users_created += 1

        # Prepare profile data
        profile_data = {
            'id': uid,
            'created_at': datetime.now().isoformat(),
            'is_recruiter': False,
            'gender': None,
            'first_name': clean_field(user.get('first_name')),
            'last_name': clean_field(user.get('last_name')),
            'country': clean_field(user.get('country')),
            'date_of_birth': clean_field(user.get('date_of_birth')),
            'phone_number': clean_field(user.get('phone_number')),
            'state': clean_field(user.get('state')),
            'city': clean_field(user.get('city')),
            'current_role_': clean_field(user.get('current_role_')),
            'email_address': email
        }

        # Remove any None values
        profile_data = {k: v for k, v in profile_data.items() if v is not None}

        try:
            # Check if profile exists
            existing_profile = supabase.table('profile').select('*').eq('id', uid).execute()
            
            if hasattr(existing_profile, 'data') and len(existing_profile.data) > 0:
                # Update existing profile
                upsert_response = supabase.table('profile').update(profile_data).eq('id', uid).execute()
            else:
                # Insert new profile
                upsert_response = supabase.table('profile').insert(profile_data).execute()

            if hasattr(upsert_response, 'data'):
                print(f"Profile {'updated' if existing_profile.data else 'created'} for user {email}")
                profiles_updated += 1
            else:
                print(f"Error upserting profile for {email}: {upsert_response}")
                errors += 1

        except Exception as e:
            print(f"Error with profile operation: {e}")
            errors += 1

        existing_interview = supabase.table('Interviews')\
            .select('*')\
            .eq('userID', uid)\
            .eq('jobID', job_id)\
            .execute()

        if hasattr(existing_interview, 'data') and len(existing_interview.data) > 0:
            print(f"Interview entry already exists for user {email} with job {job_id}")
            interviews_existing += 1
        else:
            interview_data = {
                'jobID': job_id,
                'userID': uid,
                'status': 'pending',
            }

            interview_response = supabase.table('Interviews').insert(interview_data).execute()

            if hasattr(interview_response, 'data'):
                print(f"Interview entry created for user {email} with job {job_id}")
                interviews_created += 1
            else:
                print(f"Error creating interview entry for {email}: {interview_response}")
                errors += 1

    except Exception as e:
        print(f"Exception for user {email}: {e}")
        errors += 1

print("\n=== Final Summary ===")
print(f"Total users processed: {total_users}")
print(f"Users updated: {users_updated}")
print(f"Users created: {users_created}")
print(f"Profiles updated: {profiles_updated}")
print(f"New interviews created: {interviews_created}")
print(f"Existing interviews found: {interviews_existing}")
print(f"Errors encountered: {errors}")
print("========================")