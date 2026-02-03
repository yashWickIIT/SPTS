from google import genai
from dotenv import load_dotenv
import os
from groq import Groq


load_dotenv()

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)

response = client.models.generate_content(
    model="gemini-3-pro-preview",
    contents="Explain how AI works in a few words",
)

print(response.text)
