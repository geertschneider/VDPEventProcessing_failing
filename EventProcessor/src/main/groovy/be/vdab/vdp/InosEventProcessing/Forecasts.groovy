package be.vdab.vdp.InosEventProcessing

import java.time.LocalDateTime

class ForecastsInfo {
    public class Period {
        LocalDateTime BeginPeriod
        LocalDateTime EndPeriod
    }

    Period IG
    Period OG1
    Period OG2
}
