import pandas as pd
import numpy as np
import wikipedia

print('Reading data...')
df = pd.read_csv('tags_dataset.csv')
tags = df['tag'].tolist()
tags = np.unique(tags)

descriptions = []

for i, tag in enumerate(tags):
    print(f'Checking tag {i+1}/{len(tags)}...')
    try:
        title = wikipedia.search(tag)[0]
        summary = wikipedia.summary(title, sentences=2)
    except:
        summary = ''

    descriptions.append({'tag': tag, 'description': summary})

print('Saving data...')
df = pd.DataFrame.from_records(descriptions)
df.to_csv('output.csv')
print('Done')
