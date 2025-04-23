from kafka import KafkaProducer
import json
import random
import uuid
from datetime import datetime, timedelta
import time
import os




# Sample user names
user_names = [
    "Alice Cooper", "Bob Johnson", "Charlie Smith", "David Lee", "Emily Davis",
    "Sophia Martinez", "Liam Brown", "Olivia Taylor", "Ethan Harris", "Mia Wilson"
]

# Sample post messages
post_messages = [
    "Zara’s latest collection is stunning! Can't wait to shop. 😍 #fashion",
    "Loving the new Zara styles! So trendy and elegant. 🔥",
    "Quality over everything! Zara nailed it again. ⭐⭐⭐⭐⭐",
    "Wow, the summer collection is on point! Great job, Zara!",
    "Just checked out Zara's latest collection. Some cool pieces in there.",
    "Saw Zara’s new arrivals. Not bad, might try a few.",
    "Good range of designs this season. Not sure about the prices though.",
    "Zara's prices have skyrocketed! Not worth it anymore. 😡",
    "Honestly, the new collection is disappointing. Expected better.",
    "Zara is losing its uniqueness. Everything looks the same these days.",
    "Quality is dropping while prices are rising. Not happy. 😞"
]

# Sample comment messages
comment_messages = [
    "Absolutely love this! Zara never disappoints. 😍",
    "Just got my order! Super happy with the design and fabric!",
    "Wow, this is exactly what I was looking for! 🔥🔥🔥",
    "Zara’s quality keeps improving. Worth every penny!",
    "I saw this in store, looks decent.",
    "Hmm, not sure about this. Some designs are cool, some are meh.",
    "A few nice pieces, but nothing too special this time.",
    "Too expensive for what it is. Not impressed. 😡",
    "Wish they had more colors. Feels repetitive every season.",
    "Fabric quality is not what it used to be. Disappointed. 😕",
    "Looks good in photos, but not so much in reality. 😐",
    "Stylish and comfortable! Just what I was looking for.",
    "The designs are unique this season!",
    "I've been waiting for this collection to drop!",
    "A must-have for fashion lovers!",
    "Why don’t they restock sold-out items sooner?",
    "Not happy with the size options. Wish they had more variety.",
    "The accessories this season are so chic!",
    "Formal wear collection is amazing!",
    "I love Zara’s take on classic styles.",
    "Bought a dress from this collection, and I’m in love!",
    "Good collection, but wish they had more discounts.",
    "Need more sustainable fabric choices!",
    "Casual wear is perfect for daily use!",
    "Zara never fails to impress with trendy styles.",
    "Why do they run out of stock so fast?!",
    "Winter collection is cozy and stylish!",
    "Finally, some good formal wear options!",
    "Best collection so far this year!",
    "My new favorite outfit is from this collection!",
    "Can't wait to get my hands on these new arrivals!",
    "Their online store experience needs improvement!"
]



def generate_post(created_time):
    post_id = str(uuid.uuid4())
    category = random.choice(categories)
    likes_count = random.randint(100, 1000)

    # Generate comments
    comments = []
    num_comments = random.randint(3, 5)
    for _ in range(num_comments):
        comment_id = str(uuid.uuid4())
        comment_time = created_time + timedelta(days=random.randint(0, 3), hours=random.randint(0, 23))
        commenter_name = random.choice(user_names)
        comment_message = random.choice(comment_messages)
        like_count = random.randint(1, 100)

        comments.append({
            "id": comment_id,
            "created_time": comment_time.isoformat() + "+0000",
            "from": {"id": str(uuid.uuid4()), "name": commenter_name},
            "message": comment_message,
            "like_count": like_count
        })

    message = random.choice(post_messages)

    post = {
        "id": post_id,
        "created_time": created_time.isoformat() + "+0000",
        "from": {"id": str(uuid.uuid4()), "name": "Zara Official"},
        "message": message,
        "likes": {"summary": {"total_count": likes_count}},
        "comments": {"data": comments},
       
    }

    return post

# Save the post to local file before pushing to Kafka
def save_post_to_file(post_data, created_time, folder="data"):
    os.makedirs(folder, exist_ok=True)
    date_str = created_time.strftime("%Y-%m-%d")
    file_path = os.path.join(folder, f"post_detail_{date_str}.json")
    with open(file_path, "w") as f:
        json.dump(post_data, f, indent=4)
    return file_path


if __name__ == "__main__":
    print("Starting producer... generating 15 days of data with 50 posts each")

    for i in range(15):
        created_time = datetime.utcnow() - timedelta(days=i+1)
        posts_data = []

        for _ in range(50):
            post = generate_post(created_time)
            posts_data.append(post)

        path = save_post_to_file(posts_data, created_time)
        print(f"Saved {len(posts_data)} posts to file: {path}")
        

