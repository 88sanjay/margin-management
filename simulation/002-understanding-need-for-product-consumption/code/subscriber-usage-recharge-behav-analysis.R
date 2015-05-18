## MP KL Bonus Recharge Clustering 

KPI_LIST_KL <- read.csv("~/margin-management/data/margin_KPIs/KL/KPI_LIST_KL.csv", header=F)
names(KPI_LIST_KL) <- c('msisdn','rchg_p','local_og','std_og','onnet_og','isd_og','total_og','sms','data','non_bonus_rchg_amt','bonus_rchg_amt','n_bonus_rchg_cnt','bonus_rchg_cnt','net_arpu','AON')

KPI_LIST_MP <- read.csv("~/margin-management/data/margin_KPIs/MP/KPI_LIST_MP.csv", header=F)
names(KPI_LIST_MP) <- c('msisdn','rchg_p','local_og','std_og','onnet_og','isd_og','total_og','sms','data','non_bonus_rchg_amt','bonus_rchg_amt','n_bonus_rchg_cnt','bonus_rchg_cnt','net_arpu','AON')

# piece wise summary of all combos

KPI_LIST <- KPI_LIST_KL
results <- ddply(KPI_LIST,c('rchg_p'),summarise,size=length(rchg_p))
rchg_to_filter <- results[order(-results[,2])[1:200],1]
KPI_LIST <- subset(KPI_LIST,rchg_p %in% rchg_to_filter)
get_pos <- function(x){return(mean(x[x>0]))}
get_neg <- function(x){return(mean(x[x<=0]))}
get_ratio <- function(x){return(as.double(length(x[x>0]))/length(x))}

mmnorm <- function(x) {
  x[ x > quantile(x,seq(0,1,0.01))[100]] = quantile(x,seq(0,1,0.01))[100]
  x[ x < quantile(x,seq(0,1,0.01))[2]] = quantile(x,seq(0,1,0.01))[2]
  return((x - min(x, na.rm=TRUE))/(max(x,na.rm=TRUE) - min(x, na.rm=TRUE)))
  #return(x)
}

summary_KPI = ddply(KPI_LIST,c('rchg_p'),summarise,
                    size=length(rchg_p),
                    local_og=mean(local_og),
                    std_og=mean(std_og),
                    onnet_og=mean(onnet_og),
                    isd_og=mean(isd_og),
                    total_og=mean(total_og),
                    sms=mean(sms),
                    data_in_MB= mean(data),#/1000, #for MP,
                    non_bonus_rchg_amt = mean(non_bonus_rchg_amt),
                    bonus_rchg_amt=mean(bonus_rchg_amt),
                    bonus_rchg_cnt=mean(bonus_rchg_cnt),
                    net_arpu_pos_subs = get_pos(net_arpu),
                    net_arpu_neg_subs = get_neg(net_arpu),
                    ratio_pos_net_arpu_subs = get_ratio(net_arpu),
                    net_arpu=mean(net_arpu),
                    AON=mean(AON))

write.table(summary_KPI[order(-summary_KPI$size),],file="/home/sanjay/margin-management/data/KL_RCHG_TO_BEHAV_STATS.csv",row.names=F,sep = ",")


KPI_LIST_1 = cbind(KPI_LIST_MP,circle=rep("MP",dim(KPI_LIST_MP)[1]))
KPI_LIST = cbind(KPI_LIST_KL,circle=rep("KL",dim(KPI_LIST_KL)[1]))
KPI_LIST= rbind(KPI_LIST,KPI_LIST_1)
remove(KPI_LIST_1)
KPI_LIST$data[KPI_LIST$circle == "MP"] = KPI_LIST$data[KPI_LIST$circle == "MP"]/1000

normalized_KPIs <- transform(KPI_LIST,local_og=mmnorm(local_og),
                           std_og=mmnorm(std_og),
                           onnet_og=mmnorm(onnet_og),
                           isd_og=mmnorm(isd_og),
                           total_og=mmnorm(total_og),
                           sms=mmnorm(sms),
                           data= mmnorm(data),#/1000, #for MP,
                           non_bonus_rchg_amt = mmnorm(non_bonus_rchg_amt),
                           bonus_rchg_amt=mmnorm(bonus_rchg_amt),
                           n_bonus_rchg_cnt=mmnorm(n_bonus_rchg_cnt),
                           bonus_rchg_cnt=mmnorm(bonus_rchg_cnt),
                           net_arpu=mmnorm(net_arpu),
                           AON=mmnorm(AON))

## Segmentation for circles
data <- normalized_KPIs[normalized_KPIs$circle=="MP",]
x = rep(0,15)
for (i in 1:15){
  k <- kmeans(data[,3:9],centers= i,iter.max=1000)
  x[i] = sum(k$withinss)
  print(i)
}

## Both have about 8 clusters.

data <- normalized_KPIs[normalized_KPIs$circle=="MP",]
k <- kmeans(data[,3:9],centers=8,iter.max=1000)

kpis =  KPI_LIST[KPI_LIST$circle=="MP",]
for (id in 1:8){
    s <- summary(kpis[k$cluster == id,])
    path = paste("/home/sanjay/margin-management/data/MP_cluster_",id,".csv",sep="")
    write.table(s,file=path,row.names=F,sep = ",")
    print(id)
}

