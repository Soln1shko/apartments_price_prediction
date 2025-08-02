import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import boto3
from io import StringIO
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

# Установка стиля для графиков
sns.set_style("whitegrid")

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройки для подключения к Яндекс Облаку
YC_SA_KEY_ID = os.getenv('YC_SA_KEY_ID')
YC_SA_SECRET_KEY = os.getenv('YC_SA_SECRET_KEY')
YC_STORAGE_BUCKET = os.getenv('YC_STORAGE_BUCKET')
YC_ENDPOINT_URL = os.getenv('YC_ENDPOINT_URL', 'https://storage.yandexcloud.net')

def load_dataset_from_yandex_cloud():
    """
    Загружает датасет из Яндекс Облака.
    """
    try:
        session = boto3.session.Session(
            aws_access_key_id=YC_SA_KEY_ID,
            aws_secret_access_key=YC_SA_SECRET_KEY,
            region_name="ru-central1"
        )
        s3_client = session.client(
            service_name='s3',
            endpoint_url=YC_ENDPOINT_URL
        )
        
        response = s3_client.list_objects_v2(
            Bucket=YC_STORAGE_BUCKET,
            Prefix='apartments/'
        )
        
        if 'Contents' not in response:
            print("Файлы не найдены в папке apartments/")
            return None
        
        csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        
        if not csv_files:
            print("CSV файлы не найдены")
            return None
        
        latest_file = sorted(csv_files)[-1]
        print(f"Загружаем файл: {latest_file}")
        
        response = s3_client.get_object(Bucket=YC_STORAGE_BUCKET, Key=latest_file)
        csv_content = response['Body'].read().decode('utf-8')
        
        df = pd.read_csv(StringIO(csv_content))
        print(f"Датасет успешно загружен: {len(df)} записей, {len(df.columns)} колонок")
        
        return df
    
    except Exception as e:
        print(f"Ошибка загрузки датасета: {e}")
        return None

def prepare_data(df):
    """
    Подготавливает данные для обучения модели.
    """
    data = df.copy()
    
    # Удаляем записи без цены и аномалии
    data.dropna(subset=['price_rub'], inplace=True)
    data = data[(data['price_rub'] >= 500000) & (data['price_rub'] <= 50000000)]
    
    # Заполняем пропуски
    for col in ['total_area_sqm', 'living_area_sqm', 'kitchen_area_sqm']:
        data[col] = data[col].fillna(data[col].median())
    data['floor'] = data['floor'].fillna(1)
    data['floor_total'] = data['floor_total'].fillna(data['floor_total'].median())
    data['ceiling_height_m'] = data['ceiling_height_m'].fillna(2.7)
    data['year_built'] = data['year_built'].fillna(data['year_built'].median())
    
    # Кодирование района
    le_district = LabelEncoder()
    known_districts = set(data['district'].dropna().unique())
    known_districts.add('Неизвестный')
    le_district.fit(list(known_districts))
    data['district_encoded'] = le_district.transform(data['district'].fillna('Неизвестный'))
    
    # Создаем дополнительные признаки
    data['price_per_sqm'] = data['price_rub'] / data['total_area_sqm']
    data['floor_ratio'] = data['floor'] / data['floor_total']
    data['building_age'] = 2024 - data['year_built']
    
    print(f"После очистки осталось: {len(data)} записей")
    print(f"Диапазон цен: {data['price_rub'].min():,.0f} - {data['price_rub'].max():,.0f} руб.")
    
    return data, le_district

def create_features_and_target(data):
    """
    Создает матрицы признаков и целевую переменную.
    """
    feature_columns = [
        'total_area_sqm', 'living_area_sqm', 'kitchen_area_sqm',
        'floor', 'floor_total', 'ceiling_height_m', 'year_built',
        'district_encoded', 'floor_ratio', 'building_age'
    ]
    
    X = data[feature_columns].values
    y = data['price_rub'].values
    
    print(f"Матрица признаков (X): {X.shape}")
    print(f"Целевая переменная (y): {y.shape}")
    print(f"Используемые признаки: {feature_columns}")
    
    return X, y, feature_columns

def create_regression_model(input_dim):
    """
    Создает модель регрессии на TensorFlow.
    """
    model = tf.keras.Sequential([
        tf.keras.layers.Dense(128, activation='relu', input_shape=(input_dim,)),
        tf.keras.layers.Dropout(0.3),
        tf.keras.layers.Dense(64, activation='relu'),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(1) # Выходной слой
    ])
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    return model

def predict_price(model, scaler_X, scaler_y, label_encoder, total_area, living_area, 
                  kitchen_area, floor, floor_total, ceiling_height, year_built, district):
    """
    Предсказывает цену квартиры по заданным параметрам.
    """
    try:
        # Кодируем район
        if district in label_encoder.classes_:
            district_encoded = label_encoder.transform([district])[0]
        else:
            district_encoded = label_encoder.transform(['Неизвестный'])[0]
        
        # Создаем признаки
        floor_ratio = floor / floor_total if floor_total != 0 else 0
        building_age = 2024 - year_built
        
        features = np.array([[
            total_area, living_area, kitchen_area,
            floor, floor_total, ceiling_height, year_built,
            district_encoded, floor_ratio, building_age
        ]])
        
        # Масштабируем признаки
        features_scaled = scaler_X.transform(features)
        
        # Предсказываем
        pred_scaled = model.predict(features_scaled, verbose=0)
        
        # Обратное преобразование цены
        pred_price = scaler_y.inverse_transform(pred_scaled.reshape(-1, 1))[0][0]
        
        return max(0, pred_price)
    
    except Exception as e:
        return f"Ошибка предсказания: {e}"

def main():
    df = load_dataset_from_yandex_cloud()

    if df is not None:
        clean_data, label_encoder = prepare_data(df)
        print("\nПервые 5 строк очищенных данных:")
        print(clean_data.head())

        X, y, features = create_features_and_target(clean_data)
        
        # Разделяем на train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        print(f"\nРазмер обучающей выборки: {X_train.shape[0]}")
        print(f"Размер тестовой выборки: {X_test.shape[0]}")

        # Инициализируем и обучаем скейлеры
        scaler_X = StandardScaler()
        scaler_y = StandardScaler()

        # Обучаем на ТРЕНИРОВОЧНЫХ данных и преобразуем их
        X_train_scaled = scaler_X.fit_transform(X_train)
        y_train_scaled = scaler_y.fit_transform(y_train.reshape(-1, 1)).flatten()

        # Преобразуем ТЕСТОВЫЕ данные
        X_test_scaled = scaler_X.transform(X_test)
        y_test_scaled = scaler_y.transform(y_test.reshape(-1, 1)).flatten()
        print("\nДанные успешно масштабированы.")

        # Создаем модель
        model = create_regression_model(X_train_scaled.shape[1])
        model.summary()

        # Callbacks
        early_stop = tf.keras.callbacks.EarlyStopping(
            monitor='val_loss', patience=10, restore_best_weights=True
        )
        reduce_lr = tf.keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss', factor=0.2, patience=5, min_lr=0.0001
        )

        # Обучаем модель
        print("\nНачинаем обучение модели...")
        history = model.fit(
            X_train_scaled, y_train_scaled,
            validation_data=(X_test_scaled, y_test_scaled),
            epochs=100,
            batch_size=32,
            callbacks=[early_stop, reduce_lr],
            verbose=1
        )
        print("Обучение завершено!")

        # Визуализация истории обучения
        plt.figure(figsize=(12, 5))
        plt.plot(history.history['loss'], label='Loss (train)')
        plt.plot(history.history['val_loss'], label='Loss (validation)')
        plt.title('График потерь модели')
        plt.xlabel('Эпоха')
        plt.ylabel('Loss (MSE)')
        plt.legend()
        plt.show()

        # Оценка на тестовых данных
        y_pred_scaled = model.predict(X_test_scaled)
        y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()

        # Расчет метрик
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        mean_price = np.mean(y_test)
        mape = mae / mean_price * 100

        print(f"Средняя абсолютная ошибка (MAE): {mae:,.0f} руб.")
        print(f"Корень из среднеквадратичной ошибки (RMSE): {rmse:,.0f} руб.")
        print(f"Коэффициент детерминации (R²): {r2:.3f}")
        print(f"Средняя ошибка в процентах от средней цены (MAPE): {mape:.1f}%")

        # Сравнение реальных и предсказанных значений
        print("\nПримеры предсказаний")
        for i in range(5):
            actual = y_test[i]
            predicted = y_pred[i]
            error = abs(actual - predicted)
            print(f"Реальная цена: {actual:,.0f} руб. | Предсказанная: {predicted:,.0f} руб. | Ошибка: {error:,.0f} руб.")

        #Пример использования функции предсказания
        example_price = predict_price(
            model, scaler_X, scaler_y, label_encoder,
            total_area=65.0,
            living_area=40.0, 
            kitchen_area=12.0,
            floor=5,
            floor_total=9,
            ceiling_height=2.7,
            year_built=2010,
            district="Октябрьский район"
        )

        print(f"\nПример предсказания для конкретной квартиры")
        print(f"Параметры: 65м², 5/9 этаж, 2010 год, Октябрьский район")
        
        if isinstance(example_price, (np.number, float, int)):
            print(f"Предсказанная цена: {example_price:,.0f} руб.")
        else:
            print(f"Не удалось предсказать цену. Причина: {example_price}")


if __name__ == "__main__":
    main()