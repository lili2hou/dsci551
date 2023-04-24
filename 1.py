
import pandas as pd
import numpy as np
import streamlit as st

df = pd.DataFrame({
    "a": [1, 2, 3, 4, 5],
    "b": [4, 5, 6, 7, 8]
})

st.table(df)