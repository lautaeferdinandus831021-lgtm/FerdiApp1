import os, threading, time
from flask import Flask
from dotenv import load_dotenv
from bot.engine import run_bot

load_dotenv()

app = Flask(__name__)

@app.route("/")
def home():
    return "BG-BOT v5 Running 🚀"

def start_bot():
    while True:
        try:
            run_bot()
            time.sleep(5)
        except Exception as e:
            print("Bot Error:", e)

if __name__ == "__main__":
    threading.Thread(target=start_bot, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
