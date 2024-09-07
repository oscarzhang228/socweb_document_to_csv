import csv
from pymongo import MongoClient
from dotenv import load_dotenv
import os
# Get the value of the mongo key environmental variable
load_dotenv()
mongo_key = os.environ.get('MONGO_KEY')

def export_mongodb_to_csv(db_name, collection_name, output_file):
    """
    Export all documents from a MongoDB collection to a CSV file.

    Args:
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the collection to export.
        output_file (str): The output CSV file path.

    Returns:
        None
    """
    # Initialize MongoDB client
    client = MongoClient(f'mongodb+srv://shravika16093:{mongo_key}@oud-project.qhb8ogk.mongodb.net/?retryWrites=true&w=majority&appName=OUD-Project')

    db = client[db_name]
    collection = db[collection_name]
    # Fetch all documents from the collection
    documents = collection.find()
    found_docs = []

    for doc in documents:
        found_docs.append(doc)

    if len(found_docs) == 0:
        print("No documents found in the collection.")
        return

    header_to_method = {
        "questions": get_qa_time_spent,
        "quiz": get_quiz_answer
    }

    # Open CSV file for writing
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write header row based on the keys of the first document
        first_document = found_docs[0]
        headers = list(first_document.keys())
        writer.writerow(headers)

        # Write all document rows that can be directly written
        for document in found_docs:
            # handle the special cases if needed else just return it
            row = [header_to_method.get(header, lambda x: x)(document.get(header, '')) for header in headers]
            writer.writerow(row)

    print(f"Data exported successfully to {output_file}")


def get_qa_time_spent(questions):
    """
    Parse the questions and answers from the MongoDB document and gives how much the user spent on it.

    Args:
        questions (list): List of questions and answers.

    Returns:
        list: time spend on each question. in a comma separated string.
    """
    time_spent = []
    for question in questions:
        time_spent.append(str(question.get("time", "0")))

    return ",".join(time_spent)

def get_quiz_answer(quiz):
    """
    Parse the quiz and answers from the MongoDB document and gives the answers.

    Args:
        quiz (list): List of questions and answers.

    Returns:
        list: answers for each question. in a comma separated string.
    """

    if not quiz:
        return ""

    answers = []
    for i in range(3):
        answer = quiz.get("q" + str(i), "")

        if not answer:
            answer = ""

        if isinstance(answer, dict):
            answer = str(answer)

        answers.append(answer)

    return ",".join(answers)

if __name__ == "__main__":
    # Replace these with your actual database, collection name, and output file path
    db_name = "Analytics"
    collection_name = "llms"
    output_file = "output.csv"

    export_mongodb_to_csv(db_name, collection_name, output_file)
