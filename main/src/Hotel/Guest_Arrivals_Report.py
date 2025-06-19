import uuid
import random
from datetime import datetime, timedelta
import os

def generate_guest_data(num_guests=30):
    guests_data = []
    
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                  "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                  "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]
    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sophia", "Daniel", "Olivia", "Chris", "Ava",
                   "Matthew", "Isabella", "James", "Mia", "Joseph", "Charlotte", "Robert", "Amelia", "Charles", "Harper",
                   "Mary", "Evelyn", "Thomas", "Abigail", "Richard", "Elizabeth", "Paul", "Scarlett", "Mark", "Grace"]
    
    membership_statuses = ["DIAM", "PLAT", "GOLD", "SLVR", "CLUB", "DIAM-NEW", "PLAT-NEW", "GOLD-NEW", "SLVR-NEW", "CLUB-NEW"]
    vip_codes = ["EXE", "1", "2", "3", "100", "8", "SAL", "55", "N/A"]
    booking_channels = ["WEB", "IDC", "APP", "GDS"]
    companies = ["Acme Corp", "Global Corp", "Tech Solutions", "Gov Agency", "Bright Future Co.", "Innovative Labs", "N/A"]
    travel_agents = ["Travel X", "World Tours", "Business Travel Inc.", "Gov Travel", "N/A"]
    languages = ["English", "Spanish", "French", "German", "Chinese", "Japanese"]

    for _ in range(num_guests):
        guest_id = f"G{str(uuid.uuid4())[:4].upper()}" # Shortened UUID for readability
        confirmation_number = str(random.randint(10000000, 99999999))
        last_name = random.choice(last_names)
        first_name = random.choice(first_names)
        ihg_rewards_num = str(random.randint(1000000000, 9999999999))
        points_balance = f"{random.randint(1000, 500000):,}"
        membership_status = random.choice(membership_statuses)
        vip_code = random.choice(vip_codes)
        opted_in_email = random.choice([True, False])
        has_ihg_app = random.choice([True, False])
        booking_channel = random.choice(booking_channels)
        
        company_name = random.choice(companies)
        if company_name == "N/A" and random.random() < 0.3: # Make some more N/A for companies
            company_name = "N/A"
        
        travel_agent_group = random.choice(travel_agents)
        if travel_agent_group == "N/A" and random.random() < 0.3: # Make some more N/A for travel agents
            travel_agent_group = "N/A"

        preferred_language = random.choice(languages)
        ihg_co_branded_cc = random.choice([True, False])
        
        birthday = "N/A"
        if random.random() < 0.2: # 20% chance of having a birthday
            month = str(random.randint(1, 12)).zfill(2)
            day = str(random.randint(1, 28)).zfill(2) # Avoid issues with month lengths
            birthday = f"{month}-{day}"

        social_influencer = random.random() < 0.05 # 5% chance
        big_cheese = random.random() < 0.03 # 3% chance
        heads_up = random.random() < 0.15 # 15% chance
        
        hotel_comments = "N/A"
        if random.random() < 0.3: # 30% chance of comments
            comments_pool = [
                "VIP guest, prefers quiet room.",
                "Requires connecting room, family traveling.",
                "Frequent guest, always requests high floor.",
                "First-time guest, welcome personally.",
                "Celebration stay, special amenities requested.",
                "Previously had issue with AC, check room before arrival.",
                "Anniversary trip, arrange something special.",
                "Business traveler, prefers late checkout."
            ]
            hotel_comments = random.choice(comments_pool)

        guests_data.append({
            "GuestID": guest_id,
            "ConfirmationNumber": confirmation_number,
            "LastName": last_name,
            "FirstName": first_name,
            "IHGOneRewardsNum": ihg_rewards_num,
            "PointsBalance": points_balance,
            "MembershipStatus": membership_status,
            "VIPCode": vip_code,
            "OptedInIHGEmail": opted_in_email,
            "HasIHGApp": has_ihg_app,
            "BookingChannel": booking_channel,
            "CompanyName": company_name,
            "TravelAgentGroup": travel_agent_group,
            "PreferredLanguage": preferred_language,
            "IHGCoBrandedCC": ihg_co_branded_cc,
            "Birthday": birthday,
            "SocialInfluencer": social_influencer,
            "BigCheese": big_cheese,
            "HeadsUp": heads_up,
            "HotelComments": hotel_comments
        })
    return guests_data

def generate_stays_data(guests_data):
    stays_data = []
    
    room_types = ["KBR", "DBL", "STE", "KNG", "TWN"]
    rate_categories = ["STD", "CORP", "GOV", "DISC", "PKG"]
    room_attributes_pool = [
        "1 King Bed", "2 Double Beds", "One Bedroom Suite", "Premier City View",
        "High Floor", "Pool View", "Connecting Room", "Accessible Room", "Corner Room"
    ]

    stay_id_counter = 1
    for guest in guests_data:
        num_stays = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1], k=1)[0] # Most guests have 1 stay
        for _ in range(num_stays):
            stay_id = f"S{str(stay_id_counter).zfill(4)}"
            guest_id = guest["GuestID"]
            
            check_in_date = (datetime.now() + timedelta(days=random.randint(0, 7))).strftime("%Y-%m-%d")
            nights = random.choices([1, 2, 3, 4, 5, 6, 7], weights=[0.4, 0.2, 0.15, 0.1, 0.05, 0.05, 0.05], k=1)[0]
            check_out_date = (datetime.strptime(check_in_date, "%Y-%m-%d") + timedelta(days=nights)).strftime("%Y-%m-%d")
            
            room_type = random.choice(room_types)
            room_status = random.choice(["O/C", "V/C", "O/D"])
            room_number = random.randint(100, 999)
            room_assigned = f"{room_number}/{room_status}"
            
            rate_category = random.choice(rate_categories)
            room_rate = round(random.uniform(150.00, 500.00), 2)
            num_rooms = random.choices([1, 2], weights=[0.9, 0.1], k=1)[0]
            adult_children_count = f"{random.randint(1, 4)}/{random.randint(0, 2)}"
            
            early_arrival = random.random() < 0.2
            long_stay = nights >= 5 # Define long stay as 5+ nights
            
            room_attributes = random.sample(room_attributes_pool, k=random.randint(0, 2))
            room_attributes = ", ".join(room_attributes) if room_attributes else "N/A"

            stays_data.append({
                "StayID": stay_id,
                "GuestID": guest_id,
                "CheckInDate": check_in_date,
                "CheckOutDate": check_out_date,
                "Nights": nights,
                "RoomType": room_type,
                "RoomAssigned": room_assigned,
                "RateCategory": rate_category,
                "RoomRate": room_rate,
                "NumRooms": num_rooms,
                "AdultChildrenCount": adult_children_count,
                "EarlyArrival": early_arrival,
                "LongStay": long_stay,
                "RoomAttributes": room_attributes
            })
            stay_id_counter += 1
    return stays_data

def generate_guest_actions_data(guests_data):
    actions_data = []
    
    action_types_pool = [
        "Confirmable Upgrade", "Send Upgrade Email", "Provide Lounge Access", "Upgrade Room",
        "Member Recognition", "Reward Night", "Early Arrival", "IHG Employee", "Ambassador",
        "Royal Ambassador", "Inner Circle Member", "Welcome Back", "Priority Enrollment",
        "Target for Enrollment", "Ambassador Enrollment", "Business Rewards Member",
        "Book Direct", "Close to Next Membership Status", "HeartBeat Issue",
        "New Royal Ambassador", "New Ambassador Member", "New Inner Circle Member",
        "New Status Diamond", "New Status Platinum", "New Status Gold", "New Status Silver",
        "New Member", "Recent IHG Stay", "Long Stay", "Status Match"
    ]

    action_id_counter = 1
    for guest in guests_data:
        guest_id = guest["GuestID"]
        
        # Populate all boolean flags
        for action_type in action_types_pool:
            if action_type in ["Confirmable Upgrade", "Send Upgrade Email", "Provide Lounge Access", "Upgrade Room", "Reward Night"]:
                action_status = random.random() < 0.3 # 30% chance for common actions
            elif action_type in ["HeartBeat Issue", "Close to Next Membership Status"]:
                action_status = random.random() < 0.15 # 15% chance for issues/near status
            elif "New Status" in action_type or "New Member" in action_type or "New Royal Ambassador" in action_type or "New Ambassador Member" in action_type or "New Inner Circle Member" in action_type:
                 action_status = random.random() < 0.05 # 5% chance for new statuses
            elif "Welcome Back" == action_type and guest["MembershipStatus"] != "CLUB":
                action_status = True # Likely for non-club members
            else:
                action_status = random.random() < 0.5 # Default 50% chance

            email_sent = False
            if action_type == "Send Upgrade Email" and action_status:
                email_sent = random.random() < 0.7 # 70% chance it's sent if action is true

            amb_expiration = "N/A"
            if guest["MembershipStatus"] == "DIAM" or guest["MembershipStatus"] == "PLAT" and random.random() < 0.5:
                exp_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
                amb_expiration = exp_date

            actions_data.append({
                "ActionID": f"A{str(action_id_counter).zfill(4)}",
                "GuestID": guest_id,
                "ActionType": action_type,
                "ActionStatus": action_status,
                "EmailSent": email_sent,
                "AmbassadorExpiration": amb_expiration if action_type == "Ambassador" else "N/A", # Only for Ambassador action type
                "CloseToNextStatus": action_status if action_type == "Close to Next Membership Status" else False,
                "NewRoyalAmbassador": action_status if action_type == "New Royal Ambassador" else False,
                "NewAmbassadorMember": action_status if action_type == "New Ambassador Member" else False,
                "NewInnerCircleMember": action_status if action_type == "New Inner Circle Member" else False,
                "NewStatusDiamond": action_status if action_type == "New Status Diamond" else False,
                "NewStatusPlatinum": action_status if action_type == "New Status Platinum" else False,
                "NewStatusGold": action_status if action_type == "New Status Gold" else False,
                "NewStatusSilver": action_status if action_type == "New Status Silver" else False,
                "NewMember": action_status if action_type == "New Member" else False,
                "RecentIHGStay": action_status if action_type == "Recent IHG Stay" else False,
                "IHGEmployee": action_status if action_type == "IHG Employee" else False,
                "Ambassador": action_status if action_type == "Ambassador" else False,
                "RoyalAmbassador": action_status if action_type == "Royal Ambassador" else False,
                "InnerCircleMember": action_status if action_type == "Inner Circle Member" else False,
                "WelcomeBack": action_status if action_type == "Welcome Back" else False,
                "PriorityEnrollment": action_status if action_type == "Priority Enrollment" else False,
                "TargetForEnrollment": action_status if action_type == "Target for Enrollment" else False,
                "AmbassadorEnrollment": action_status if action_type == "Ambassador Enrollment" else False,
                "BusinessRewardsMember": action_status if action_type == "Business Rewards Member" else False,
                "BookDirect": action_status if action_type == "Book Direct" else False,
                "StatusMatch": action_status if action_type == "Status Match" else False
            })
            action_id_counter += 1
    return actions_data

def generate_preferences_requests_data(guests_data):
    preferences_data = []
    
    special_requests_pool = [
        "AE-ROOM AWAY FROM ELEVATOR REQUESTED", "NS-NONSMOKING ROOM REQUESTED",
        "HF-HIGH FLOOR REQUESTED", "QUIET ROOM REQUESTED", "CONNECTING ROOM NEEDED",
        "ROLLAWAY BED", "CRIB REQUESTED", "EARLY CHECK-IN REQUESTED", "LATE CHECK-OUT REQUESTED", "N/A"
    ]
    previously_used_amenities_pool = [
        "Dinner in the Hotel Restaurant", "Spa Service", "Kids Club",
        "Fitness Center", "Pool Access", "Room Service", "Laundry Service", "N/A"
    ]
    guest_interests_pool = [
        "Reading", "Hiking", "Coffee", "Italian Cuisine", "Running", "Cycling",
        "Japanese Food", "Theme Parks", "Swimming", "Fast Food", "Art", "Museums",
        "Golf", "Skiing", "Shopping", "Live Music", "Cooking"
    ]
    kimpton_preference_types = [
        "Favorite Sweet", "Favorite Newspaper", "Favorite Liquid", "Favorite Munchies", "Other Favorites"
    ]
    kimpton_preference_values = {
        "Favorite Sweet": ["Dark Chocolate", "Fruit Tart", "Cheesecake", "Gelato"],
        "Favorite Newspaper": ["Wall Street Journal", "New York Times", "USA Today", "Local Paper"],
        "Favorite Liquid": ["Sparkling Water", "Espresso", "Green Tea", "Fresh Orange Juice"],
        "Favorite Munchies": ["Mixed Nuts", "Local Cheese Plate", "Dried Fruit", "Popcorn"],
        "Other Favorites": ["Extra Pillows", "Slippers", "Bathrobe", "Humidifier"]
    }

    preference_id_counter = 1
    for guest in guests_data:
        guest_id = guest["GuestID"]
        
        # Special Requests
        if random.random() < 0.6: # 60% chance of 1-2 special requests
            num_requests = random.randint(1, 2)
            selected_requests = random.sample(special_requests_pool, k=min(num_requests, len(special_requests_pool) -1)) # -1 to avoid N/A if selecting multiple
            for req in selected_requests:
                if req != "N/A":
                    preferences_data.append({
                        "PreferenceID": f"P{str(preference_id_counter).zfill(4)}",
                        "GuestID": guest_id,
                        "RequestType": "Special Request",
                        "RequestDescription": req,
                        "PreviouslyUsedAmenity": "N/A",
                        "GuestInterest": "N/A",
                        "KimptonPreferenceType": "N/A",
                        "KimptonPreferenceValue": "N/A"
                    })
                    preference_id_counter += 1

        # Previously Used Amenities
        if random.random() < 0.5: # 50% chance of 1-3 previously used amenities
            num_amenities = random.randint(1, 3)
            selected_amenities = random.sample(previously_used_amenities_pool, k=min(num_amenities, len(previously_used_amenities_pool)-1))
            for amenity in selected_amenities:
                if amenity != "N/A":
                    preferences_data.append({
                        "PreferenceID": f"P{str(preference_id_counter).zfill(4)}",
                        "GuestID": guest_id,
                        "RequestType": "Previously Used Amenity",
                        "RequestDescription": "N/A",
                        "PreviouslyUsedAmenity": amenity,
                        "GuestInterest": "N/A",
                        "KimptonPreferenceType": "N/A",
                        "KimptonPreferenceValue": "N/A"
                    })
                    preference_id_counter += 1

        # Guest Interests
        if random.random() < 0.7: # 70% chance of 1-3 guest interests
            num_interests = random.randint(1, 3)
            selected_interests = random.sample(guest_interests_pool, k=min(num_interests, len(guest_interests_pool)))
            for interest in selected_interests:
                preferences_data.append({
                    "PreferenceID": f"P{str(preference_id_counter).zfill(4)}",
                    "GuestID": guest_id,
                    "RequestType": "Guest Interest",
                    "RequestDescription": "N/A",
                    "PreviouslyUsedAmenity": "N/A",
                    "GuestInterest": interest,
                    "KimptonPreferenceType": "N/A",
                    "KimptonPreferenceValue": "N/A"
                })
                preference_id_counter += 1

        # Kimpton Preferences (only for DIAM/PLAT members or specific VIP codes)
        if guest["MembershipStatus"] in ["DIAM", "PLAT"] or guest["VIPCode"] in ["EXE", "SAL"]:
            for pref_type in kimpton_preference_types:
                if random.random() < 0.7: # 70% chance to have a Kimpton preference if eligible
                    pref_value = random.choice(kimpton_preference_values[pref_type])
                    preferences_data.append({
                        "PreferenceID": f"P{str(preference_id_counter).zfill(4)}",
                        "GuestID": guest_id,
                        "RequestType": "Kimpton",
                        "RequestDescription": "N/A",
                        "PreviouslyUsedAmenity": "N/A",
                        "GuestInterest": "N/A",
                        "KimptonPreferenceType": pref_type,
                        "KimptonPreferenceValue": pref_value
                    })
                    preference_id_counter += 1
    return preferences_data

def generate_guest_satisfaction_data(guests_data):
    satisfaction_data = []
    
    heartbeat_comments_pool = [
        "Bathroom faucet was leaking last stay.",
        "Long wait for check-in last time at another IHG hotel.",
        "Room cleanliness issues reported.",
        "Excellent service from front desk staff!",
        "Breakfast was delicious, great variety.",
        "N/A", "N/A", "N/A", "N/A" # More N/A to simulate fewer comments
    ]
    heartbeat_survey_issues_pool = [
        "Room cleanliness", "Slow check-in", "Noise from adjacent room",
        "Issues with Wi-Fi", "Restaurant service speed", "HVAC malfunction",
        "Pool area maintenance", "N/A", "N/A", "N/A" # More N/A
    ]
    fb_spend_categories = ["$", "$$", "$$$", "$$$$"]
    fb_ratings = ["1/poor", "2/below average", "3/average range stay", "4/above average", "5/excellent"]

    satisfaction_id_counter = 1
    for guest in guests_data:
        satisfaction_id = f"GS{str(satisfaction_id_counter).zfill(4)}"
        guest_id = guest["GuestID"]
        
        heartbeat_score = round(random.uniform(5.0, 10.0), 1)
        overall_experience = "green" if heartbeat_score >= 7.5 else "red"
        
        heartbeat_comments = random.choice(heartbeat_comments_pool)
        
        problem_at_my_hotel = False
        heartbeat_survey_issue = "N/A"
        if overall_experience == "red" or random.random() < 0.1: # Small chance of problem even if green
            problem_at_my_hotel = True
            heartbeat_survey_issue = random.choice(heartbeat_survey_issues_pool)

        avg_fb_spend = random.choice(fb_spend_categories)
        avg_fb_rating = random.choice(fb_ratings)

        satisfaction_data.append({
            "SatisfactionID": satisfaction_id,
            "GuestID": guest_id,
            "OverallExperience": overall_experience,
            "HeartBeatScore": heartbeat_score,
            "HeartBeatComments": heartbeat_comments,
            "ProblemAtMyHotel": problem_at_my_hotel,
            "HeartBeatSurveyIssue": heartbeat_survey_issue,
            "AvgFandBSepend": avg_fb_spend,
            "AvgFandBRating": avg_fb_rating
        })
        satisfaction_id_counter += 1
    return satisfaction_data

def generate_stay_history_summary_data(guests_data):
    history_data = []
    
    ihg_hotels = ["ATL", "LAX", "NYC", "SFO", "DEN", "ORD", "CHI", "MCO", "SEA", "AUS", "BOS", "MIA", "DAL", "WAS"]
    brands = ["Kimpton", "Holiday Inn", "Crowne Plaza", "Indigo", "InterContinental", "Hotel Indigo", "Staybridge Suites"]

    history_id_counter = 1
    for guest in guests_data:
        history_id = f"H{str(history_id_counter).zfill(4)}"
        guest_id = guest["GuestID"]
        
        stays_my_hotel_12m = random.randint(0, 10)
        stays_all_ihg_12m = stays_my_hotel_12m + random.randint(0, 20)
        
        last_3_stays_locations = []
        num_last_stays = random.randint(0, 3)
        for _ in range(num_last_stays):
            location = random.choice(ihg_hotels)
            stay_date = (datetime.now() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
            last_3_stays_locations.append(f"{location} ({stay_date})")
        last_3_stays_locations = ", ".join(last_3_stays_locations) if last_3_stays_locations else "N/A"

        brand_most_visited = f"{random.choice(brands)} ({random.randint(10, 90)}%)"

        reservations_2wks_out = []
        num_upcoming_res = random.choices([0, 1, 2], weights=[0.6, 0.3, 0.1], k=1)[0]
        for _ in range(num_upcoming_res):
            location = random.choice(ihg_hotels)
            res_date = (datetime.now() + timedelta(days=random.randint(1, 14))).strftime("%Y-%m-%d")
            reservations_2wks_out.append(f"{location} ({res_date})")
        reservations_2wks_out = ", ".join(reservations_2wks_out) if reservations_2wks_out else "N/A"

        history_data.append({
            "HistoryID": history_id,
            "GuestID": guest_id,
            "StaysMyHotel_12M": stays_my_hotel_12m,
            "StaysAllIHG_12M": stays_all_ihg_12m,
            "Last3StaysLocations": last_3_stays_locations,
            "BrandMostVisited": brand_most_visited,
            "Reservations2WksOut": reservations_2wks_out
        })
        history_id_counter += 1
    return history_data

def generate_corporate_rate_amenities_data(guests_data):
    corporate_amenities_data = []
    
    amenities_pool = [
        "Complimentary Breakfast", "Late Check-out", "Parking Discount",
        "Executive Lounge Access", "Complimentary Wi-Fi", "Conference Room Access",
        "Gym Access", "Dry Cleaning Discount"
    ]

    corp_amenity_id_counter = 1
    for guest in guests_data:
        if guest["CompanyName"] != "N/A" and random.random() < 0.6: # 60% chance for corporate guests to have amenities
            num_amenities = random.randint(1, 3)
            selected_amenities = random.sample(amenities_pool, k=num_amenities)
            amenity_description = ", ".join(selected_amenities)

            corporate_amenities_data.append({
                "CorpAmenityID": f"CA{str(corp_amenity_id_counter).zfill(4)}",
                "GuestID": guest["GuestID"],
                "CompanyName": guest["CompanyName"],
                "AmenityDescription": amenity_description
            })
            corp_amenity_id_counter += 1
    return corporate_amenities_data

# Generate all data
guests_data = generate_guest_data(num_guests=30)
stays_data = generate_stays_data(guests_data)
guest_actions_data = generate_guest_actions_data(guests_data)
preferences_requests_data = generate_preferences_requests_data(guests_data)
guest_satisfaction_data = generate_guest_satisfaction_data(guests_data)
stay_history_summary_data = generate_stay_history_summary_data(guests_data)
corporate_rate_amenities_data = generate_corporate_rate_amenities_data(guests_data)

# Define the base directory for saving CSV files
base_dir = '/Users/jpetrides/Documents/Demo Data/Hotels/main/data/Hotel/GAR'

# Create the directory if it doesn't exist
os.makedirs(base_dir, exist_ok=True)

# Helper function to write data to a CSV file
def write_to_csv(data, filename):
    if not data:
        print(f"No data to write for {filename}. Skipping CSV creation.")
        return

    filepath = os.path.join(base_dir, filename)
    keys = data[0].keys() # Get headers from the first dictionary

    with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"Successfully wrote {len(data)} rows to {filepath}")

# Write each dataset to a CSV file
write_to_csv(guests_data, 'guests.csv')
write_to_csv(stays_data, 'stays.csv')
write_to_csv(guest_actions_data, 'guest_actions.csv')
write_to_csv(preferences_requests_data, 'preferences_requests.csv')
write_to_csv(guest_satisfaction_data, 'guest_satisfaction.csv')
write_to_csv(stay_history_summary_data, 'stay_history_summary.csv')
write_to_csv(corporate_rate_amenities_data, 'corporate_rate_amenities.csv')
