import numpy as np
import pandas as pd
from joblib import load
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

class GWAModelRandomForest:
    def __init__(self,  n_estimators=100, random_state=42):
        self.model = None
        self.program = None
        self.train_length = None
        self.df = None
        self.n_estimators = n_estimators
        self.random_state = random_state

    def train(self, known_gwas, program):
        self.program = program
        self.train_length = len(known_gwas)

        cumulative_gwa = np.cumsum(known_gwas) / \
            np.arange(1, self.train_length + 1)

        self.model = RandomForestRegressor(
            n_estimators=self.n_estimators, random_state=self.random_state)

        df = pd.DataFrame({
            'y':  known_gwas,
            'program': program,
            'semester': np.arange(1, self.train_length + 1),
            'cumulative_gwa': cumulative_gwa
        })

        X = df[['program', 'semester', 'cumulative_gwa']]
        y = df['y']
        self.model.fit(X, y)
        self.df = df

        # Display the processed DataFrame
        print("\nProcessed DataFrame:")
        print(df.head())  # Show first 5 rows

    def forecast(self, forecast_horizon=1):
        total_periods = self.train_length + forecast_horizon
        future = pd.DataFrame({
            'program': self.program,
            'semester': np.arange(1, total_periods + 1),
            'cumulative_gwa': np.concatenate([self.df['cumulative_gwa'], np.repeat(self.df['cumulative_gwa'].iloc[-1], forecast_horizon)])
        })

        X_future = future[['program', 'semester', 'cumulative_gwa']]
        y_future = self.model.predict(X_future)
        forecast_future = pd.DataFrame(
            {'yhat': y_future}).iloc[-forecast_horizon:]
        predictions = forecast_future['yhat'].tolist()
        # Predictions will be in correct order
        predictions = [np.clip(p, 1.0, 5.0)
                       for p in predictions]  # To avoid error

        return predictions

    def evaluate(self, known_gwas, program, test_size=0.2):
        if len(known_gwas) < 2:
            print("Not enough data for evaluation.")
            return None
        split_index = max(1, int(len(known_gwas) * (1 - test_size)))
        train_data = known_gwas[:split_index]
        test_data = known_gwas[split_index:]
        self.train(train_data, program)
        predictions = self.forecast(len(test_data))
        mse = mean_squared_error(test_data, predictions)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(test_data, predictions)
        mape = np.mean(np.abs(
            (np.array(test_data) - np.array(predictions)) / np.array(test_data))) * 100
        pe = np.mean(
            ((np.array(test_data) - np.array(predictions)) / np.array(test_data))) * 100

        print("\nModel Performance on Evaluation Set:")
        print(f"  MAE: {mae:.2f}")
        print(f"  MSE: {mse:.2f}")
        print(f"  RMSE: {rmse:.2f}")
        print(f"  MAPE: {mape:.2f}%")
        print(f"  PE: {pe:.2f}%")

        if mape < 5:
            indicator = "Excellent"
        elif mape < 10:
            indicator = "Good"
        elif mape < 20:
            indicator = "Fair"
        else:
            indicator = "Poor"
        print(f"  Performance Indicator: {indicator}")

        return mse, rmse, mae, mape, pe

# Load the pre-trained model from joblib file
loaded_rf_model = load('C:/LEONAIDAS/FORECASTING/random_forest_model.joblib')

# Create a new instance and set its model attribute to the loaded model
model = GWAModelRandomForest(n_estimators=100, random_state=42)
model.model = loaded_rf_model

# Define your input data - you can customize these values
known_gwas = [1.72, 2.11, 2.06, 2.13, 1.25, 2.00]
program_id = 21  # Bachelor of Technical-Vocational Teacher Education
forecast_horizon = 1  # Number of semesters to forecast

# Train the model with known data first (this will use the loaded model internally)
model.train(known_gwas, program_id)

# Use forecast instead of predict
predictions = model.forecast(forecast_horizon)

# Print results
print("\nKnown GWA values:", [f"{val:.2f}" for val in known_gwas])
print(f"Predicted GWA for the next semester:", [f"{val:.2f}" for val in predictions])

# Forecast summary
if len(predictions) > 0:
    last_known = known_gwas[-1]
    first_pred = predictions[0]
    if first_pred < last_known:
        print("\nIndicator: Your GWA is predicted to improve.")
    elif first_pred > last_known:
        print("\nIndicator: Your GWA is predicted to worsen.")
    else:
        print("\nIndicator: Your GWA is predicted to remain the same.")

start_sem = len(known_gwas) + 1
print("\nUpcoming Semesters Prediction Summary:")
for idx, pred in enumerate(predictions, start=start_sem):
    print(f"  Semester {idx}: Predicted GWA: {pred:.2f}")

print("\nModel evaluation on the training data:")
model.evaluate(known_gwas, program_id, test_size=0.2)