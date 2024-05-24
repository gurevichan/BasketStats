from scraper import get_team_data, df_for_print_groupby, filter_df
import pandas as pd
import os
import streamlit as st
st.set_page_config(layout='wide')


if __name__ == "__main__":
    team_url = "https://basket.co.il/team.asp?TeamId=1054&lang=en"
    df = get_team_data(team_url)
    filtered_df = filter_df(df)
    left_column, right_column = st.columns(2)
    by_coach_btn = left_column.button('Player Stats by Coach')
    by_round_btn = right_column.button('Player Stats by Round')
    # by_coach_btn = st.button("Player Stats by Coach")
    # by_round_btn = st.button("Player Stats by Round")
    # get min round number for each coach
    if by_coach_btn:
        st.write("Player stats by coach")
        coach_buttons = {}
        coaches_list = list(df.groupby("coach")["game_idx"].min().sort_values().index)
        # coaches_checkbox = st.checkbox(coaches_list)

        for coach in coaches_list:
            st.write(coach)
            st.dataframe(df_for_print_groupby(filtered_df[filtered_df["coach"] == coach], sort_by='min'), width=1200)
    # st.dataframe(df_for_print_groupby(filtered_df), width=1200) # set width to 800 for wide page
    if by_round_btn:
        st.write("Player stats by round")

        # st.write("2nd round Kantouris")
        # st.dataframe(df_for_print_groupby(filtered_df[filtered_df["game_idx"] <= 24], sort_by='min'), width=1200)
        st.write("2nd round Jonathan Alon")
        # min_game_idx_number_when_jonathan_alon_is_coach
        min_game_idx = filtered_df[filtered_df['coach'] == 'Jonathan Alon']['game_idx'].min()
        st.dataframe(df_for_print_groupby(filtered_df[(filtered_df["game_idx"] <= 24) & (filtered_df["game_idx"] >= min_game_idx)], sort_by='min'), width=1200) # set width to 800 for wide page
        st.write("Upper house")
        st.dataframe(df_for_print_groupby(filtered_df[filtered_df["game_idx"] > 24], sort_by='min'), width=1200) # set width to 800 for wide page
