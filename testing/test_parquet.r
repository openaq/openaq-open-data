### test_parquet.r ---
##
## Filename: test_parquet.r
## Description:
## Author: Christian A. Parker
## Maintainer:
## Created: Thu Dec 23 07:43:21 2021 (-0800)
## Version:
## Last-Updated:
##           By:
##     Update #: 28
## URL:
## Keywords:
## Compatibility:
##
######################################################################
##
### Commentary:
##
## Explore
##
######################################################################
##
### Change Log:
##
##
######################################################################
##
### Code:

library(arrow);
library(dplyr)


## create some fake data to play with
## station, datetime, value

days <- c(1, 7, 30, 60, 120, 365)
measurands <- c("pm25"=11111, "pm10"=22222, "co"=33333, "wd"=44444, "ws"=55555, "temp"=66666)

get_file_size <- function(fpath) {
    bytes <- file.size(fpath)
    return(bytes/1024);
}

check_parquet <- function(df) {
    fpath <- './testfile.parquet'
    write_parquet(df, fpath)
    return(get_file_size(fpath));
}

check_csv <- function(df, compress=FALSE) {
    if(compress) {
        fpath <- './testfile.csv.gz'
        write.csv(df, gzfile(fpath), row.names=FALSE)
    } else {
        fpath <- './testfile.csv'
        write.csv(df, fpath, row.names=FALSE)
    }
    return(get_file_size(fpath));
}


results <- data.frame(t(sapply(rev(days), function(dy) {
    print(sprintf("Processing %s days", dy))
    n <- dy * 24 * 60
    datetimes <- as.character(seq(from=as.POSIXct('2021-01-01 00:00:00', tz='UTC'), by='1 min', length.out=n));
    ms <- rep(names(measurands), each=n)
    dts <- rep(datetimes, times=length(measurands))
    ids <- rep(measurands, each=n)
    df <- data.frame(
        location_id = 1,
        sensor_id = ids,
        location = 'station #1',
        datetime = dts,
        lon = -122.658722,
        lat = 45.512230,
        measurand = ms,
        value = rnorm(n, 20, 5)
    )
    ## write the file
    parquet <- check_parquet(df);
    csv <- check_csv(df, FALSE);
    csv.gz <- check_csv(df, TRUE);
    c(n = n*length(measurands), days = dy, parquet = parquet, csv = csv, csv.gz = csv.gz)
})))
## repeat

## review data
png("simulated_results.png")
plot(I(csv/1024)~days, results, type="n", xaxt='n', log="yx")
ax2 <- axTicks(2)
plot(
    I(csv/1024)~days, results,
    type="n",
    main=sprintf("Simulated file size based on 1 minute data for %s measurands", length(measurands)),
    xlab="Days (Records)",
    ylab="File Size (mb)",
    xaxt='n',
    yaxt='n',
    ylim=c(0.05, max(results$csv/1024)),
    log="xy"
)
axis(2, at = c(0.1, ax2), labels = c("0.1", sprintf("%.f", ax2)))
axis(1, at = days)
axis(1, at = days, tick=FALSE, line = 1, labels = sprintf("(%sK)", round((days * 60 * 24 * length(measurands))/1000), 0))
rect(xleft=min(results$days), ybottom=100, xright=max(results$days), ytop=200, col='gray95', border=NA)
text(x = min(results$days), y = 200 - 40, labels = "Suggested ideal file size range", pos=4, col="gray60")
lines(I(csv/1024)~days, results, type='b', col='tomato', pch = 19)
lines(I(parquet/1024)~days, results, type='b', col='lightblue', pch = 19)
lines(I(csv.gz/1024)~days, results, type='b', col='steelblue', pch=19)
legend("bottomright", legend=c("csv","csv.gz","parquet"), pch=15, cex=1.5, pt.cex=2, col = c("tomato","steelblue","lightblue"), bty='n')
dev.off()

## models
lm(csv~n, results)


######################################################################
### test_parquet.r ends here
