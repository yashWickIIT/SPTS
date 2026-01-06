from google import genai

client = genai.Client(api_key="AIzaSyBZWLR-l1_diQMWk5R2lXHbCZNuzg1qRC8")

response = client.models.generate_content(
    model="gemini-3-pro-preview",
    contents="Explain how AI works in a few words",
)

print(response.text)
