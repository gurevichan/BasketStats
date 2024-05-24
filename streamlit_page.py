from scraper import get_team_data, df_for_print_groupby, filter_df, TeamScraper
import pandas as pd
import os
import streamlit as st
st.set_page_config(layout='wide')


if __name__ == "__main__":
    teams: list = [
                "https://basket.co.il/team.asp?TeamId=1054&lang=en",
                "https://basket.co.il/team.asp?TeamId=1051&lang=en",
    ]
    st.title("Basketball Stats")
    columns = st.columns(len(teams))
    teams_dict = {}
    for i, url in enumerate(teams):
        team = TeamScraper(url)
        teams_dict[team.name] = team
    radio_select = st.radio("Teams:", list(teams_dict.keys()), index=len(teams_dict)-1)
    df = None
    df = get_team_data(team_scraper=teams_dict[radio_select])

    if df is not None:
        st.subheader(team.name)
        filtered_df = filter_df(df)
        left_column, right_column = st.columns(2)
        table_select = st.radio("Tables to show:", ['Player Stats by Coach', 'Player Stats by Round'], index=0)
        if table_select == 'Player Stats by Coach':
            st.write("Player stats by coach")
            coach_toggle = {}
            coaches_list = list(df.groupby("coach")["game_idx"].min().sort_values().index)
            coach_row = st.columns(len(coaches_list))
            for i, coach in enumerate(coaches_list):
                coach_toggle[coach] = coach_row[i].toggle(coach, value=True)
            # options = st.multiselect("Coaches:", coaches_list, coaches_list[-1:])
            for coach, toggle in coach_toggle.items():
                if toggle:
                    st.write(coach)
                    st.dataframe(df_for_print_groupby(filtered_df[filtered_df["coach"] == coach], sort_by='min'), width=1200)
        if table_select == 'Player Stats by Round':
            st.subheader("Player stats by round")
            st.write("2nd round Jonathan Alon")
            # min_game_idx_number_when_jonathan_alon_is_coach
            min_game_idx = filtered_df[filtered_df['coach'] == 'Jonathan Alon']['game_idx'].min()
            st.dataframe(df_for_print_groupby(filtered_df[(filtered_df["game_idx"] <= 24) & (filtered_df["game_idx"] >= min_game_idx)], sort_by='min'), width=1200) # set width to 800 for wide page
            st.write("Upper house")
            st.dataframe(df_for_print_groupby(filtered_df[filtered_df["game_idx"] > 24], sort_by='min'), width=1200) # set width to 800 for wide page
