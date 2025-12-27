def generate_output_filename(s3_incoming_filename):
    path_parts = s3_incoming_filename.split('/')
    return f'protected/{path_parts[-1]}'

def encoding_email(URL, s3, chunksize):
    key = Fernet.generate_key()  
    cipher_suite = Fernet(key)

    try:
        obj = s3.get_object(Bucket=URL.get('bucket'), Key=URL.get('key'))
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as tmp_file:
            first_chunk = True
            
            for chunk in pd.read_csv(io.BytesIO(obj['Body'].read()), chunksize=chunksize):
                chunk['email'] = chunk['email'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode())
                chunk.to_csv(tmp_file, index=False, header=first_chunk)
                first_chunk = False
            
            tmp_file_path = tmp_file.name
        
        with open(tmp_file_path, 'rb') as f:
            output_filename = generate_output_filename(URL.get('key'))
            s3.put_object(Bucket=URL.get('bucket'), Key=output_filename, Body=f)
            print(f"Файл загружен: {output_filename}")
        
        os.unlink(tmp_file_path)
        print("Временный файл удален.")
        
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Ошибка загрузки в S3: {e}")
    except Exception as e:
        print(f"Общая ошибка: {e}")
