import pandas as pd
import io
from sqlalchemy import create_engine
import os

# DB connection from env or fallback
pg_host = os.getenv('POSTGRES_HOST','localhost')
pg_port = os.getenv('POSTGRES_PORT','5432')
pg_db = os.getenv('POSTGRES_DB','test1')
pg_user = os.getenv('POSTGRES_USER','migration_user')
pg_password = os.getenv('POSTGRES_PASSWORD','StrongPassword123')

engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

# Create a bad file content: first row is a long paragraph with many commas and repeated phrases
paragraph = (
    "DISEASE INFORMATION: Ulcerative Colitis OVERVIEW: Ulcerative Colitis presents with various clinical manifestations that healthcare providers must carefully evaluate, "
    "Diagnosis typically involves a combination of physical examination, laboratory tests, and imaging studies. Early detection is crucial for effective management, "
    "research has made significant advances in recent years, leading to improved diagnostic tools and treatment options. "
)
# repeat to enlarge
first_line = paragraph + paragraph
# create CSV-like with commas so pandas thinks many columns
bad_content = first_line + "\n" + "This is row2, with more, commas, here" + "\n"

print('Bad content sample length:', len(bad_content))

# Simulate reading as text file
text_content = bad_content

# Try parsing like app
try:
    df = pd.read_csv(io.StringIO(text_content), sep='\t')
    if len(df.columns) == 1:
        df = pd.read_csv(io.StringIO(text_content), sep=',')
    if len(df.columns) == 1:
        df = pd.read_csv(io.StringIO(text_content), sep=r'\s+', engine='python')
except Exception as e:
    print('Initial parsing failed:', e)
    try:
        df = pd.read_csv(io.StringIO(text_content))
    except Exception as e2:
        print('Fallback parsing failed:', e2)
        df = pd.DataFrame({'content':[text_content]})

print('Columns detected:', df.columns)

# Normalize function from app (copied)
import re

def normalize_dataframe(df, file_content=None):
    try:
        rows, cols = df.shape
    except Exception:
        rows, cols = (0,0)
    if (rows==0 or cols==0) and file_content:
        return pd.DataFrame({'content':[file_content]})
    cols_list = [str(c) for c in df.columns]
    dup = len(cols_list) != len(set(cols_list))
    long_name = any(len(c) > 120 for c in cols_list)
    too_many = len(cols_list) > 100
    if dup or long_name or too_many:
        if file_content is None:
            try:
                file_content = '\n'.join(df.astype(str).agg(' '.join, axis=1).tolist())
            except Exception:
                file_content = ' '.join(df.astype(str).values.flatten().astype(str).tolist())
        return pd.DataFrame({'content':[file_content]})
    new_cols=[]
    seen={}
    for i,c in enumerate(cols_list):
        c2 = re.sub(r'[^0-9a-zA-Z_]','_',c).strip('_').lower()
        if not c2:
            c2 = f'col_{i+1}'
        base=c2
        suffix=1
        while c2 in seen:
            suffix+=1
            c2 = f"{base}_{suffix}"
        seen[c2]=True
        new_cols.append(c2)
    df.columns = new_cols
    return df

ndf = normalize_dataframe(df, file_content=text_content)
print('Normalized columns:', ndf.columns)

# Try to_sql
try:
    ndf.to_sql('simulate_badfile', engine, schema='public', if_exists='replace', index=False)
    print('to_sql succeeded, rows:', len(ndf))
    # cleanup
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS public.simulate_badfile')
except Exception as e:
    import traceback
    print('to_sql failed:', e)
    print(traceback.format_exc())
