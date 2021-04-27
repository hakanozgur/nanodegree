## About Project
This project explores the PISA 2018 dataset
The dataset contains several questionnaires answered by 600k students 100k teachers and 22k schools
Each student in this dataset also participates in a cognitive exam that may contain math, reading, science, and financial literacy questions.

### About PISA
PISA assesses the extent to which 15-year-old students have acquired key knowledge and skills that are essential for full participation in modern society.
- what is important for citizens to know and be able to do?  
- Main focus is not what you know, but what you do with what you know.

### Issues with submission
- I used the library called Plotly for visualizations
- it uses an insane amount of memory and CPU. But the interactive charts and smooth graphs made it OK.
- Unfortunately, I didn't realize that the file size was also increasing. The final size of the notebook was over 300MBs. 
- I realized it too late. So I put the screenshots instead of charts themselves.
- The code is there but commented out.
- Also the data used is large for submission
- The raw data used at the beginning was 6.2GB, the filtered final version is around 96MB
- I only put the final version. If required I could submit versions between raw and the final


### Findings from exploratory analysis
- I spend most of the time in the provided Codebook. It helped me answer basic questions about the dataset and form a bit complicated scenarios
- After downloading the dataset and going through exploration in the first notebooks that start with `01_data_gathering` and `02_data_wrangling` I got the general idea about the questions I want to ask
- In `02_data_wrangling` and `03_data_wrangling_cleaning` notebooks when I wondered about some combination of features I created a new column for them

### List of resources
- I sepend some of the time in these links
  - http://www.oecd.org/pisa/data/2018database/
  - https://plotly.com/python/plotly-fundamentals/
  - https://www.kaggle.com/kanncaa1/plotly-tutorial-for-beginners
  - https://github.com/streamlit/streamlit
  - https://www.oecd.org/pisa/keyfindings/pisa-2012-results-overview.pdf
  - http://www.oecd.org/pisa/PISA%202018%20Insights%20and%20Interpretations%20FINAL%20PDF.pdf
  - and I don't know how many quick google quiestions

### Data selection process

- I was going to select my dataset. One that I have been collecting from a local site like Reddit. However looking at the provided datasets, I found a gem. Got curious and decided to work on the PISA.
- I only heard about PISA from the news. When our country ranking changed a couple of years ago. 
- Now looking at their website I am blown away by this vast open dataset, well-thought questions, data explorers, research documentation, technical reports, procedures, proposals. This is insane. I haven't seen anything like it.
- Not just the large data and reports. The quality of them is amazing. Well thought of questionnaires, asking questions not just to students but also parents, teachers, and school administrators.
- There are strategic vision proposals that question what should young people know and be able to do about science in the future. How science should work with health, fairness, personal development, meaningful work, rationale, ethics, and values.
- This proposal is for PISA 2024. It is about people that will graduate in 2030-2035! Which skills they need to be successful in 2030 to 2050. The document is up and available now.
- Seeing this document reminded me of Ken Robinson's TED talk. 
- He said "Nobody has a clue what the world will look like in five years' time, yet we're meant to educate them["children"] for it."
- Not an easy task.
- Apparently, OECD has a ~400m euro budget. PISA is just one of their programs. Amazingly, the research is open and accessible.  

### Side Quests
- use one of plotly/bokeh/streamlit visualization tools

### Notes
jupyter.exe nbconvert .\05_data_presentation.ipynb --to slides --template .\scripts\output_toggle.tpl --post serve