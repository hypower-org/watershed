(ns physicloud.core)

;add in kernel creation



@(net/cpu :10.10.10.5 {:10.10.10.5 {:edges [:10.10.10.3]} :10.10.10.3 {:edges [:10.10.10.5]}} 2 :provides [:cpu-1-data] :requires [:cpu-2-data])















