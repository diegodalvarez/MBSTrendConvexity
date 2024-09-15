# MBS Trend Convexity
The idea is to use a CTA-like model to hedge the MBS index. The idea is that most participants try to hedge the MBS TBAs via duration-matched Treasury Futures. While duration-matching is seen as ideal it loses out on possible trend that may be occuring within other Treasuries. This notebook will look at optimizing the tradeoff between trend and duration-matching. For the most part its assumed to be long MBS and short a Treasury Future. The main goal to this model is to show that the classical MBS Treasury spread which can be harvested will underperform a strategy that trades the Treasury's duration vs Trend. 

## Data Used
1. Bloomberg "LUMS" Mortgage Backed Security Index
2. FV Roll Adjusted Futures
3. TU Roll Adjusted Futures
4. TY Roll Adjusted Futures
5. UXY Roll Adjusted Futures
6. WN Roll Adjusted Futures

It should be noted that all of this data is sourced from Bloomberg Terminal, and uses their roll adjusted methodology. Another consideration is the use of Treasury futures that are not likely to match such as TU and WN. Although its fair to assume that the scenario of MBS duration matching TU duration its extremely unlikely. Rather than taking out futures contracts that are not likely to match all will be incorporated to ensure that the model is truly robust. 

## Start with generic (constant Treasury) basis
![image](https://github.com/user-attachments/assets/092f34b9-7961-40c8-9dc7-0dc1ec87ce22)
It is evident that there are some sample size problems since contracts such as FV & TY have existed for a long time while contracts like WN and UXY are newer. The cumulative return performance is not a full representation of how the basis would really return. In this case the MBS is hedged via lagged duration which is optimized to avoid having any duration. The optimization assumes that there isn't any leverage and fully invested. 
When comparing the cumulative return over the same window almost all contracts return the same amount. There is a considerable amount of disperion between strategies that likely occurs to convexity mismatches. The third graph is the duration of our duration-hedged basis which shows how there is a convexity mismatch occurs. 

Since its not possible to get MBS Convexity for LUMS index on Bloomberg Terminal plotting the Treasury CTD convexity vs. Average Absolute Duration Exposure of each basis shows

![image](https://github.com/user-attachments/assets/76a5c34d-849a-4cd2-96af-b892c3c202a7)

The contracts that have the greatest amount of duration mismatch is driven by convexity. Its also a bit obvious that the contracts are placed with respect to their assumed duration with TU being all the way on the left, TY & UXY grouped together and WN far out. 
