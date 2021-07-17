#!/usr/bin/env python
# coding: utf-8

# In[ ]:



def getPMR(PMR_SKey):
      return PMR_SKey
    
def getConditions(dftotal,PMR_SKey):
    dftotal2= dftotal.filter(dftotal["PMR_SKey"] == PMR_SKey)
    Pdftotal2 = dftotal2.toPandas()
    conditions_list=[]
    for i in range(len(Pdftotal2.iloc[:,:-20].columns)):
        if Pdftotal2.iloc[0,i] == 1:
              conditions_list.append(Pdftotal2.columns[i])
      
    conditions_list = [i for i in conditions_list]
    return conditions_list
  
def getRules(dfRule,dfFactTriggeredRules,PMR_SKey):
    df = dfFactTriggeredRules.filter(dfFactTriggeredRules.PMR_SKey == PMR_SKey)
    df = df.join(dfRule,on='Rule_SKey')
    rules_list=list(df.select('Rule_BusKey').toPandas()['Rule_BusKey'])
    return rules_list
  
def getPCA(dfTotal,PMR_SKey):
    df = dfTotal
    df = df.filter(df['PMR_SKey'] == PMR_SKey)
    return df.select('PC1','PC2','PC3','PC4','PC5','PC6','PC7','PC8','PC9','PC10',
                    'PC11','PC12','PC13','PC14','PC15','PC16','PC17','PC18','PC19','PC20').toPandas()
  
def KeyPCs(dftotal, Conditions):
    Key=[]
    for C in Conditions:
​
        dftotal['label'] = np.arange(dftotal.shape[0])
        dftotal.loc[(dftotal[str(C)] == 1), 'label'] = str(C)
        dftotal.loc[(dftotal[str(C)] == 0), 'label'] = 'None'
​
        label = dftotal['label']
  
        dftotal = dftotal.drop('label',axis =1)
  
        pca = dftotal[["PC1","PC2","PC3","PC4","PC5","PC6","PC7","PC8","PC9","PC10",
                "PC11","PC12","PC13","PC14","PC15","PC16","PC17","PC18","PC19","PC20"]]
​
        X = pca
        y = label
  
        model = DecisionTreeClassifier()
         # fit the model
        model.fit(X, y)
        # # # get importance
        importance = model.feature_importances_
        Key.append(np.array([importance.argmax(),importance.max()]))
    return np.array(Key)
  
​
​
def DBScanData(dftotal,key):
#   if dfsmall.filter(dfsmall.PMR_SKey == PMR).count() == 0:
#     dfsmall = dfsmall.union(dftotal.filter(dftotal.PMR_SKey == PMR))
    columns=[]
  
    for i in key:
        columns.append(f.col(df.columns[int(i)]))
​
    output = dftotal.withColumn("PCs", f.array(columns))
 
    return output
​
def GetSmallerDataset(dftotal,conditions_list,ratio,pmr,random_state=42):
    dftotal['label_count'] = dftotal[conditions_list].sum(axis=1)
  
    
    dftotal2 = dftotal.loc[dftotal['label_count'] > 0].sample(frac = ratio)
​
    if len(dftotal2.loc[dftotal2['PMR_SKey'] == pmr]) == 0:
        dftotal3 = pd.concat([dftotal2, dftotal.loc[dftotal['PMR_SKey'] == pmr]], ignore_index=True)
        return dftotal3
    else:
        return dftotal2
  
​
​
​
def GetRuleCluster(dftotal,KeyPCs,eps,min_pts,conditions_list):
    
    dftotal2 = dftotal.filter(col(conditions_list[0]))
    for condition in conditions_list[1:]:
        dftotal3 = dftotal.filter(col(str(condition)) == 1)
        dftotal2 = dftotal2.union(dftotal3)
      
    dfpca = dftotal2.select("PC1","PC2","PC3","PC4","PC5","PC6","PC7","PC8","PC9","PC10",
                "PC11","PC12","PC13","PC14","PC15","PC16","PC17","PC18","PC19","PC20")
    
    PCs_iter =  list(itertools.combinations(KeyPCs,2))
  
    for i in PCs_iter:
        columns = [f.col(dfpca.columns[int(i[0])-1]),f.col(dfpca.columns[int(i[1])-1])] 
​
        output = dfpca.withColumn("PCs", f.array(columns)).select('PMR_SKey','PCs').withColumnRenamed('PCs','value').withColumnRenamed('PMR_SKey','id')
​
        df_clusters = process(df=output, epsilon=eps, min_pts=min_pts, dist= distance.euclidean,dim= 2, checkpoint_dir=None)
      
    return df_clusters
#       zipped = zip(i,df_clusters)
#     dictionary = dict(zipped)
#     return dictionary
#       ## this is population clusters, but for patient KeyPCs
    
    
def getRuleLabels(PMR_SKey,df_clusters,xdf):
  
    df_clusterTotal = xdf.join(df_clusters,xdf.id == df_clusters.point,how = 'outer')
    clusterRanks = df_clusters.groupBy('component').count().sort('count',ascending = False).toPandas()
    PatientGroup = df_clusterTotal.filter(col('PMR_SKey') == PMR_SKey)
  
    PatientGroup =PatientGroup.toPandas()
    Patientcluster = PatientGroup.component[0]
  
    PatientRules = dfFactTriggeredRules.filter(col("PMR_SKey") == PMR_SKey)
    PatientRules = PatientRules.toPandas()
  
    PatientRules = PatientRules.Rule_SKey
  
    dfClusterRule = df_clusterTotal.join(dfFactTriggeredRules.select('PMR_SKey','Rule_SKey'), on = 'PMR_SKey',how='inner')
  
    dfclusters = dfClusterRule.groupby('component').pivot('Rule_SKey').count()
  
    Pdfclusters = dfclusters.toPandas().T.fillna(0)
    Pdfclusters.columns = Pdfclusters.iloc[0]
    Pdfclusters = Pdfclusters.iloc[1:,:]
  
    clusterIndex = Pdfclusters.columns.get_loc(Patientcluster)
    ClusterRulesPatient = Pdfclusters.iloc[:,:clusterIndex+1]
  
    Pdfclusters = Pdfclusters[clusterRanks.component]
  
    rule_label=np.array([])
    for rule_index in range(PatientRules.shape[0]):
        rule_label = np.append(rule_label, -1)
        for col_index in range(ClusterRulesPatient.shape[1]):
​
            for df_index in range(ClusterRulesPatient.shape[0]):
                if ClusterRulesPatient.iloc[df_index,col_index]> 0:
                    if int(ClusterRulesPatient.index[df_index]) == PatientRules[rule_index]:
                        if rule_label[rule_index] == -1:
                            rule_label[rule_index] = col_index
              
  ###appending to dffacttriggeredrules
    PatientRules = pd.DataFrame(PatientRules)
    PatientRules['importance_label'] = rule_label
    SPatientRules = spark.createDataFrame(PatientRules)
    SPatientRules = SPatientRules.join(dfRule[['Rule_SKey','RuleName']],on = 'Rule_SKey')
    return SPatientRules
    

