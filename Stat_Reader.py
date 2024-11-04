# Eshwar Umarengan and Yug Purohit
# Intermediate Data Programming
# Stat Reader File

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
import plotly

def main():
    '''
    Initalizes the Stat_Reader object
    '''
    reader = Stat_Reader()


class Stat_Reader():
    def __init__(self):
        df_list = []
        for file in os.listdir("data"):
            import_data = self.load_in_data(file)
            edited_data = self.data_edit(import_data, file)
            df_list.append(edited_data)
        self.combined_df = self.combine_data(df_list)
        self.players_df = self.player_rating_generator()
        self.improvement_rating_df = self.calculate_improvement_rating()
        self.ten_most_improved = self.improvement_rating_df.nlargest(10, 'improvement_rating')
        self.ten_least_improved = self.improvement_rating_df.nsmallest(10, 'improvement_rating')
        self.graph_improvement()    
        self.decline_age_df = self.calculate_decline_age()
        self.five_youngest_decline = self.decline_age_df.nsmallest(5, 'decline_age')
        self.five_oldest_decline = self.decline_age_df.nlargest(5, 'decline_age')
        self.graph_decline()
        

    def load_in_data(self, csv_file_name):
        '''
        Loads in data from file, then creates dataframe with the raw data
        '''
        df = pd.read_csv("data/" + csv_file_name)
        return df

    
    def data_edit(self, df, csv_file_name):
        '''
        Takes dataframe of raw data and then edits and cleans it to fit parsing criteria
        This is done through helper methods
        '''
        # first 5 datasets don't have TOPG statistic, so we're creating it using TOr statistic and other data using a formula
        if ('TOr' in df.columns) and ('TOPG' not in df.columns):
            df = df[[
                'FULL NAME', 'TEAM', 'POS', 'AGE', 'GP', 'MPG', '3P%', '3PA',
                '2P%', '2PA', 'FT%', 'FTA', 'RPG', 'BPG', 'APG', 'SPG', 'TOr'
            ]]

            df['TOPG'] = round((df['TOr'] / (100 - df['TOr']) *
                                (df['2PA'] + df['3PA'] + 0.44 * df['FTA'])), 2)

        df = df[[
            'FULL NAME', 'TEAM', 'POS', 'AGE', 'GP', 'MPG', '3P%', '3PA',
            '2P%', '2PA', 'FT%', 'FTA', 'RPG', 'BPG', 'APG', 'SPG', 'TOPG'
        ]]
        df = df.fillna(0)
        names = list(df['FULL NAME'])
        for name in names:
            # case to deal with players that are traded midseason
            # multiple instances of one player combined into one player
            if names.count(name) > 1:
                combined_player = self.player_combine(name, df)
                df = df.drop(df[df['FULL NAME'] == name].index)
                df = df.append(combined_player, ignore_index=True)
        # players playing less than 41 games aren't eligible, so taken out
        df = df.mask(df['GP'] <= 41)
        df = df.dropna()
        # customizing columns of dataset to match year
        # ex: PPG to PPG_3 in year 3 dataset
        df = self.change_cols(df, csv_file_name)
        return df


    def player_combine(self, name, df):
        '''
        combines player instances of the same name on different teams within a season to calculate one player stat           line
        '''
        # summarizes each statistic
        names_df = df.drop(df[df['FULL NAME'] != name].index)
        full_name = name
        team = names_df['TEAM'].max()
        pos = names_df['POS'].max()
        age = names_df['AGE'].max()
        gp = names_df['GP'].sum()
        mpg = (names_df['MPG'] * names_df['GP']).sum() / gp
        three_per = round((names_df['3P%'] * names_df['GP']).sum() / gp, 2)
        three_att = round((names_df['3PA'] * names_df['GP']).sum() / gp, 2)
        two_per = round((names_df['2P%'] * names_df['GP']).sum() / gp, 2)
        two_att = round((names_df['2PA'] * names_df['GP']).sum() / gp, 2)
        ft_per = round((names_df['FT%'] * names_df['GP']).sum() / gp, 2)
        ft_att = round((names_df['FTA'] * names_df['GP']).sum() / gp, 2)
        rpg = round((names_df['RPG'] * names_df['GP']).sum() / gp, 2)
        bpg = round((names_df['BPG'] * names_df['GP']).sum() / gp, 2)
        apg = round((names_df['APG'] * names_df['GP']).sum() / gp, 2)
        spg = round((names_df['SPG'] * names_df['GP']).sum() / gp, 2)
        topg = round((names_df['TOPG'] * names_df['GP']).sum() / gp, 2)

        # returns dictionary of summarized statistics
        player_dict = {
            'FULL NAME': full_name,
            'TEAM': team,
            'POS': pos,
            'AGE': age,
            'GP': gp,
            'MPG': mpg,
            '3P%': three_per,
            '3PA': three_att,
            '2P%': two_per,
            '2PA': two_att,
            'FT%': ft_per,
            'FTA': ft_att,
            'RPG': rpg,
            'BPG': bpg,
            'APG': apg,
            'SPG': spg,
            'TOPG': topg
        }

        return player_dict


    def change_cols(self, df, file_name):
        '''
        changes the column names of each dataset to include the year after each stat
        ex: The points per game column in year 5 is renamed PPG_5 from PPG
        '''
        # parsing out the year using file_name
        file_name = file_name[:-4]
        year = file_name[5:]
        df.columns = [column + "_" + year for column in df.columns]
        return df


    def combine_data(self, df_list):
        '''
        Combines the ten datasets (from df_list) into one big dataset
        Fencepost algorithm is used and each two consecutive datasets are merged (outer join) and gradually added to big dataset
        '''
        concat_df = df_list[0]
        col_name = concat_df.columns[0][-3:]
        start_year = col_name[col_name.index("_") + 1:]

        for df_add in df_list[1:]:
            add_col = df_add.columns[0][-3:]
            add_year = add_col[add_col.index("_") + 1:]
            concat_df = concat_df.merge(df_add,
                                        left_on='FULL NAME_' + start_year,
                                        right_on='FULL NAME_' + add_year,
                                        how='outer')

        return concat_df

    def player_rating_generator(self):
        '''
        Generates a player ratings dataframe
        Consists of 11 columns, name and the 10 ratings from each year
        '''
        players_df = pd.DataFrame(columns=[
            'name', 'rating_1', 'rating_2', 'rating_3', 'rating_4', 'rating_5',
            'rating_6', 'rating_7', 'rating_8', 'rating_9', 'rating_10'
        ])
        # runs through each player
        for i in range(0, len(self.combined_df)):
            player_series = self.combined_df.iloc[i]
            player_dict = {}
            # runs through each year
            for year in range(1, 11):
                year = str(year)
                # if player has played this year, player rating for that year is created using helper method
                if pd.notnull(player_series['FULL NAME_' + year]):
                    player_dict['name'] = player_series['FULL NAME_' + year]
                    player_dict['rating_' +
                                year] = self.calculate_player_rating(
                                    player_series, year)

                # if player hasn't played that year, 0 is put instead
                else:
                    player_dict['rating_' + year] = 0

            players_df = players_df.append(player_dict, ignore_index=True)

        return players_df

  
    def calculate_player_rating(self, series, year):
        '''
        player method to calculate player rating for a specific player in a specific year
        formula is based on fantasy basketball tool
        '''
        rating = round(3 * (series['3P%_' + year] * series['3PA_' + year]), 2)
        rating += round(2 * (series['2P%_' + year] * series['2PA_' + year]), 2)
        rating += round(1 * (series['FT%_' + year] * series['FTA_' + year]), 2)
        rating += round(
            (series['RPG_' + year] * 1.2) + (series['APG_' + year] * 1.5) +
            (series['BPG_' + year] * 2) + (series['SPG_' + year] * 2) +
            (series['TOPG_' + year] * -1), 2)

        return rating

  
    def calculate_improvement_rating(self):
        '''
        generates a improvement ratings dataframe
        consists of 2 columns, name and improvement_rating
        calculates improvement rating for each player
        '''
        improvement_dict = {}
        name_list = []
        improvement_list = []
        # goes through each player
        for i in range(0, len(self.players_df)): 
            player_series = self.players_df.iloc[i]   
            differences_list = []
            first = player_series['rating_1'] 
            # calculates differences between 2 consecutive player ratings (2 consecutive years)
            # adds this to list of differences
            for index in range(2, player_series.size):
                second = player_series['rating_' + str(index)]
                differences_list.append(second - first)
                first = player_series['rating_' + str(index)]
            # removes player ratings from years not played
            differences_list.remove(0)
            # average of differences is assigned to that player in dict (which is converted to df)
            improvement_dict[player_series['name']] = round((sum(differences_list) / len(differences_list)), 2)
            name_list.append(player_series['name'])
            improvement_list.append(round((sum(differences_list) / len(differences_list)), 2))
            list_of_tuples = list(zip(name_list, improvement_list))
        
        improvement_df = pd.DataFrame(list_of_tuples, columns = ['name', 'improvement_rating'])
        return improvement_df
      

    def graph_improvement(self):
        '''
        Function to graph ten most improved players and ten least improved players in separate histograms and files
        Note: HTML files must be downloaded to be viewed
        '''
        fig1 = px.histogram(self.ten_most_improved, x='name', y = 'improvement_rating')
        fig1.update_layout(bargap=0.2)
        fig1.update_layout(title_text='Ten Most Improved Players')
        plotly.offline.plot(fig1, filename='ten_most_improved.html')
      
        fig2 = px.histogram(self.ten_least_improved, x='name', y = 'improvement_rating')
        fig2.update_layout(bargap=0.2)
        fig2.update_layout(title_text='Ten Least Improved Players')
        plotly.offline.plot(fig2, filename='ten_least_improved.html')
        
    
    def calculate_decline_age(self): 
        '''
        looks at player ratings for each player
        if a player's player rating has went down 3 years in a row
        they qualify as declining and a new dataframe with declining 
        players are created and the age they declined at 
        '''
        name_list = []
        decline_list = []
        for i in range(0, len(self.players_df)):
            player_series = self.players_df.iloc[i]
            rating = 0
            count = 0
            # goes through each rating in the player series
            for i in range(1, 11):
                #if a player has a rating that year
                if (player_series['rating_'+str(i)] > 0):
                    #if that rating is smaller than the previous one(decline)
                    if (player_series['rating_'+str(i)] < rating):
                        count += 1
                        
                    #if the rating is higher
                    else:
                        count = 1
                    rating = player_series['rating_'+str(i)]
                if count == 3:

                    name_list.append(player_series['name'])
                    
                    decline_year = self.combined_df.loc[self.combined_df['FULL NAME_'+str(i)] == player_series['name']]
                    decline_list.append(round(decline_year.iloc[0]['AGE_'+str(i)]))
                    break

        list_of_tuples = list(zip(name_list, decline_list))
        decline_df = pd.DataFrame(list_of_tuples, columns = ['name', 'decline_age'])
             
        return decline_df

    def graph_decline(self):
        '''
        This function calculates and graphs the following:
        Decline age frequency, 5 youngest declining players, 
        and 5 oldest declining players
        Note: HTML files must be downloaded to be viewed
        '''
        decline_list = self.decline_age_df['decline_age']
        avg_age = decline_list.sum()/len(decline_list)
        print("average decline age: "+str(avg_age))

        print(self.decline_age_df)
        
        age_dict = {}
        for val in self.decline_age_df['decline_age']:

            if val in age_dict:
                age_dict[val] += 1

            else:
                age_dict[val] = 1
              
        fig = go.Figure(data=[go.Pie(labels=list(age_dict.keys()), values=list(age_dict.values()))])
        fig.update_layout(title_text='Percents of Decline Ages')
        
        plotly.offline.plot(fig, filename='decline_pie_chart.html')
        
        fig2 = px.histogram(self.five_youngest_decline, x='name', y = 'decline_age')
        fig2.update_layout(bargap=0.2)
        fig2.update_layout(title_text='Five Youngest Declining Players')
        plotly.offline.plot(fig2, filename='five_youngest_players.html')

        fig3 = px.histogram(self.five_oldest_decline, x='name', y = 'decline_age')
        fig3.update_layout(bargap=0.2)
        fig3.update_layout(title_text='Five Oldest Declining Players')
        plotly.offline.plot(fig3, filename='five_oldest_players.html')


    def printer(self):
        '''
        function to test all main datasets by printing the first rows of each dataset
        '''
        print(self.combined_df.head())
        print(self.players_df.head())
        print(self.improvement_rating_df.head())
        print(self.decline_age_df.head())
        
             
if __name__ == "__main__":
    main()
