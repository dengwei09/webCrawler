def get_ip_address():
    # 3: Use OS specific command
    import subprocess , platform
    ipaddr=''
    os_str=platform.system().upper()

    if os_str=='LINUX' :

        # Linux:
        arg='ip route list'    
        p=subprocess.Popen(arg,shell=True,stdout=subprocess.PIPE)
        data = p.communicate()
        sdata = data[0].split()
        ipaddr = sdata[ sdata.index('src')+1 ]
        #netdev = sdata[ sdata.index('dev')+1 ]
        return ipaddr
        #return ipaddr

    elif os_str=='WINDOWS' :

        # Windows:
        arg='route print 0.0.0.0'    
        p=subprocess.Popen(arg,shell=True,stdout=subprocess.PIPE)
        data = p.communicate()
        strdata=data[0].decode()
        sdata = strdata.split()

        while len(sdata)>0:
            if sdata.pop(0)=='Netmask' :
                if sdata[0]=='Gateway' and sdata[1]=='Interface' :
                    ipaddr=sdata[6]
                    break   
        return ipaddr
