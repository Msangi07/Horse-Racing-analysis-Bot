import pandas as pd
import os
import shutil
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

# Create a directory for downloads
DOWNLOAD_DIR = r'C:\Users\Msangi\Documents\My boy Jack\downloads'

# Remove the existing directory if it exists
if os.path.exists(DOWNLOAD_DIR):
    shutil.rmtree(DOWNLOAD_DIR)  # This will delete the directory and its contents
os.makedirs(DOWNLOAD_DIR)

# Function to analyze horse race data
def analyze_race_data(file_path):
    try:
        df = pd.read_csv(file_path, header=None, skiprows=[0])
    except pd.errors.EmptyDataError:
        raise ValueError("The file is empty or has no valid data.")
    except pd.errors.ParserError:
        raise ValueError("Error parsing the file. Please check the file format.")

    if df.empty:
        raise ValueError("No columns to parse from file. Please ensure the file has valid data.")

    df.drop(columns=[0], inplace=True)

    numeric_columns = [
        6,  # Career Wins
        5,  # Career Runs
        4,  # Handicap Rating
        10, # Career Place Strike Rate
        25, # Barrier
        119,# Last Start Finish Position
        77, # Jockey This Season Place Strike Rate
        110,# Trainer This Season Place Strike Rate
        50, # This Condition Place Strike Rate
        44, # This Track Distance Place Strike Rate
        122,# Last Start Distance Change
        19  # Average Prize Money
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    horses = []
    for index, row in df.iterrows():
        career_wins = row[6]  # Career Wins
        career_runs = row[5]  # Career Runs

        strike_rate = (career_wins / career_runs * 100) if career_runs > 0 else 0

        horse_info = {
            "Name": row[1],  # Horse Name
            "Age": row[2],  # Age
            "Handicap Rating": row[4],  # Handicap Rating
            "Career Wins": career_wins,  # Career Wins
            "Career Runs": career_runs,  # Career Runs
            "Career Place Strike Rate": row[10],  # Career Place Strike Rate
            "Barrier" : row[25], # Barrier
            "Last Start Finish Position": row[119],  # Last Start Finish Position
            "Last Start Margin": row[120],  # Last Start Margin
            "Jockey This Season Place Strike Rate": row[77],  # Jockey This Season Strike Rate
            "Trainer This Season Place Strike Rate": row[110],  # Trainer This Season Place Strike Rate
            "This Condition Place Strike Rate": row[50],  # This Condition Place Strike Rate
            "This Track Distance Place Strike Rate": row[44],  # This Track Distance Place Rate
            "Last Start Distance Change": row[122],  # Last Start Distance Change
            "Average Prize Money": row[19],  # Average Prize Money
            "Best Fixed Odds": row[21],  # Best Fixed Odds
            "Strike Rate": strike_rate  # Add strike rate to horse_info
        }
        horses.append(horse_info)

    results = []
    for horse in horses:
        result = (f"**{horse['Name']}**\n"
                  f"- Age: {horse['Age']}\n"
                  f"- Handicap Rating: {horse['Handicap Rating']}\n"
                  f"- Career Wins: {horse['Career Wins']} out of {horse['Career Runs']} ({horse['Strike Rate']:.2f}% strike rate)\n"
                  f"- Career Place Strike Rate: {horse['Career Place Strike Rate']}%\n"
                  f"- Barrier:{horse['Barrier']}\n"
                  f"- Last Start Finish Position: {horse['Last Start Finish Position']}\n"
                  f"- Last Start Margin: {horse['Last Start Margin']}\n"
                  f"- Last Start Distance Change: {horse['Last Start Distance Change']}\n"
                  f"- Jockey This Season Place Strike Rate: {horse['Jockey This Season Place Strike Rate']}%\n"
                  f"- Trainer This Season Place Strike Rate: {horse['Trainer This Season Place Strike Rate']}%\n"
                  f"- This Condition Place Strike Rate: {horse['This Condition Place Strike Rate']}%\n"
                  f"- This Track Distance Place Strike Rate: {horse['This Track Distance Place Strike Rate']}%\n"
                  f"- Best Fixed Odds: {horse['Best Fixed Odds']}\n"
                  f"- Average Prize Money: ${horse['Average Prize Money']}\n")

        # Star rating calculation
        stars = 0
        if horse["Career Place Strike Rate"] >= 50.0:
            stars += 1
        if horse["Barrier"] <= 6.0:
            stars += 1    
        if horse["Jockey This Season Place Strike Rate"] >= 35.0:
            stars += 1
        if horse["Trainer This Season Place Strike Rate"] >= 20.0:
            stars += 1
        if horse["This Condition Place Strike Rate"] >= 20.0:
            stars += 1
        if horse["This Track Distance Place Strike Rate"] >= 20.0:
            stars += 1
        if horse["Last Start Distance Change"] <= -500:
            stars += 1
        if horse["Last Start Finish Position"] <= 4:
            stars += 1       

        result += f"- Star Rating: {'★' * stars}{'☆' * (8 - stars)}\n"
        results.append(result)

    # Conclusion logic
    conclusion = "### Conclusion ###\n"
    filtered_performers = [
        horse for horse in horses 
        if horse["Last Start Finish Position"] <= 3 
        and horse["This Condition Place Strike Rate"] >= 0
        and horse["This Track Distance Place Strike Rate"] >= 0
        and horse["Career Place Strike Rate"] > 50
        and horse["Barrier"] <= 6
        and horse["Jockey This Season Place Strike Rate"] >= 35
        and horse["Trainer This Season Place Strike Rate"] >= 20
    ]
    top_performers = sorted(filtered_performers, key=lambda x: x["Career Wins"], reverse=True)[:3]

    if top_performers:
        for performer in top_performers:
            conclusion += f"- {performer['Name']} appears to be a strong contender based on their performance.\n"
    else:
        conclusion += "- No strong contenders found based on the specified criteria.\n"

    results.append(conclusion)

    # Print each result for debugging
    for res in results:
        print(res)

    return "\n".join(results)

# Function to send messages in chunks
async def send_message_in_chunks(update: Update, message: str):
    max_length = 4096
    for i in range(0, len(message), max_length):
        await update.message.reply_text(message[i:i + max_length], parse_mode='Markdown')

# Start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Welcome! Please send me a CSV file containing horse race data.")

# Handle file upload
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    original_file_name = document.file_name
    
    print(f"Received file: {original_file_name}")

    if not original_file_name.endswith('.csv'):
        await update.message.reply_text("Please upload a valid CSV file.")
        return

    file = await document.get_file()
    download_path = os.path.join(DOWNLOAD_DIR, original_file_name)

    await file.download_to_drive(download_path)
    await update.message.reply_text("File received! Analyzing the data...")

    file_size = os.path.getsize(download_path)
    print(f"Downloaded file size: {file_size} bytes")
    
    if file_size == 0:
        await update.message.reply_text("The uploaded file is empty. Please upload a valid CSV file.")
        return

    try:
        analysis_results = analyze_race_data(download_path)
        print("Analysis results generated.")  # Debugging log
        await send_message_in_chunks(update, analysis_results)
    except Exception as e:
        print(f"Error during analysis: {e}")  # Log the error
        await update.message.reply_text(f"An error occurred during analysis: {e}")
    finally:
        if os.path.exists(download_path):
            os.remove(download_path)

# Main function to run the bot
def main():
    TOKEN = '7264844505:AAGY5vMdy5nsuMJ8BRg0JYQkiDIsbj36J-g'  # Replace with your bot token
    application = ApplicationBuilder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), handle_document))

    application.run_polling()

if __name__ == "__main__":
    main()