"""A deterministic built-in pool of 500 Indian display names."""

FIRST_NAMES = [
    "Aarav", "Aadhya", "Aanya", "Abhay", "Aditya", "Advait", "Aisha", "Akash",
    "Akhil", "Akshara", "Aman", "Amaya", "Amit", "Amrita", "Ananya", "Aniket",
    "Anil", "Anisha", "Ankit", "Ansh", "Anushka", "Arjun", "Arnav", "Asha",
    "Ashish", "Avani", "Ayush", "Bhavna", "Chetan", "Deepa", "Deepak", "Dev",
    "Diya", "Esha", "Gaurav", "Geeta", "Harish", "Isha", "Jatin", "Kajal",
    "Karan", "Kavya", "Kiran", "Krishna", "Kunal", "Lakshmi", "Manish", "Meera",
    "Mohit", "Naina",
]

SURNAMES = [
    "Sharma", "Verma", "Gupta", "Singh", "Kumar", "Patel", "Yadav", "Mishra",
    "Jha", "Sinha",
]

INDIAN_RANDOM_NAMES = [f"{first} {surname}"
                       for surname in SURNAMES
                       for first in FIRST_NAMES]

assert len(INDIAN_RANDOM_NAMES) == 500
