#![allow(warnings, unused)]
extern crate tokio;

//new
pub mod serialization;
pub mod consumer;
pub mod manager;
pub mod producer;
pub mod channel;
pub mod shared;
pub mod consumer_producer;
use serde::{Deserialize, Serialize};


async fn boot_up_func() {
    console_subscriber::init();
    //let mut set = JoinSet::new();

    let manager = manager::manager();

    loop{//System dies without this loop
    }
}

#[tokio::main]
//#[async_std::main]
async fn main() {
    //async_std::task::block_on(boot_up_func());
    boot_up_func().await;
    //manager::temp_spawn_operators().await;
}


/*
no delay/sleep.
unoptimized_unrestricted:
experiment 3
 back and forth
-500:
Total runtime: 14722.
Benchmarking times in each iteration: [5637.0, 4346.0, 4629.0]

-1000
Total runtime: 29566.
Benchmarking times in each iteration: [9864.0, 9974.0, 9705.0]

-1500
Total runtime: 43935.
Benchmarking times in each iteration: [15247.0, 14176.0, 14453.0]

-2000
Total runtime: 59595.
Benchmarking times in each iteration: [21317.0, 18896.0, 19202.0]

-2500:
Total runtime: 74121.
Benchmarking times in each iteration: [25551.0, 24103.0, 24136.0]

-3000:
Total runtime: 88405.
Benchmarking times in each iteration: [30131.0, 28629.0, 29288.0]
-----------------------------------------------------------------------------------------------------------
optimized_unrestricted:

-500:
Total runtime: 15768.
Benchmarking times in each iteration: [5616.0, 5090.0, 5058.0]

-1000
Total runtime: 30805.
Benchmarking times in each iteration: [10966.0, 10058.0, 9775.0]

-1500
Total runtime: 43979.
Benchmarking times in each iteration: [15764.0, 14459.0, 13751.0]

-2000
Total runtime: 59051.
Benchmarking times in each iteration: [20282.0, 19140.0, 19620.0]

-2500:
Total runtime: 72369.
Benchmarking times in each iteration: [25332.0, 23610.0, 23419.0]

-3000:
Total runtime: 88019.
Benchmarking times in each iteration: [29492.0, 28954.0, 29561.0]
-----------------------------------------------------------------------------------------------------------
optimized_restricted:

-500:
Total runtime: 13282.
Benchmarking times in each iteration: [4716.0, 4424.0, 4122.0]


-1000
Total runtime: 29526.
Benchmarking times in each iteration: [9905.0, 10084.0, 9529.0]

-1500
Total runtime: 42499.
Benchmarking times in each iteration: [14578.0, 14358.0, 13551.0]

-2000
Total runtime: 58030.
Benchmarking times in each iteration: [19873.0, 18868.0, 19277.0]

-2500:
Total runtime: 74564.
Benchmarking times in each iteration: [28000.0, 23784.0, 22772.0]

-3000:
Total runtime: 86792.
Benchmarking times in each iteration: [30173.0, 28414.0, 28195.0]
-----------------------------------------------------------------------------------------------------------
unoptimized_restricted:

-500:
Total runtime: 13201.
Benchmarking times in each iteration: [4329.0, 4459.0, 4402.0]

-1000
Total runtime: 29848.
Benchmarking times in each iteration: [10692.0, 9729.0, 9417.0]

-1500
Total runtime: 42720.
Benchmarking times in each iteration: [15010.0, 13303.0, 14397.0]

-2000
Total runtime: 56364.
Benchmarking times in each iteration: [20065.0, 18050.0, 18237.0]

-2500:
Total runtime: 70907.
Benchmarking times in each iteration: [24136.0, 23550.0, 23210.0]

-3000:
Total runtime: 86193.
Benchmarking times in each iteration: [30029.0, 28054.0, 28099.0]

*/

/*
10 - 10 - 60 (prod3). 30 (consumer)
experiment 2
one way 50: 255853,507942,760398,1009199
optimized:

50
Total runtime: 153672.
Benchmarking times in each iteration: [51170.0, 51296.0, 51188.0]

100
Total runtime: 306270.
Benchmarking times in each iteration: [102254.0, 102040.0, 101966.0]

150
Total runtime: 458369.
Benchmarking times in each iteration: [152251.0, 152948.0, 153163.0]

200
Total runtime: 610279.
Benchmarking times in each iteration: [203673.0, 203474.0, 203122.0]

--
oneway 50: 275480,591536,917943,1249673
unoptimized:
50
Total runtime: 200651.
Benchmarking times in each iteration: [55010.0, 54941.0, 55604.0]

100
Total runtime: 432415.
Benchmarking times in each iteration: [115543.0, 117945.0, 117848.0]

150
Total runtime: 664230.
Benchmarking times in each iteration: [180688.0, 186776.0, 182779.0]

200
Total runtime: 906667.
Benchmarking times in each iteration: [244094.0, 251591.0, 249337.0]

*/

/*
http://www.plotvar.com/line.php?title=Benchmark+Experiment+1&yaxis=Time+(ms)&xaxis=test&xvalues=50%2C100%2C150%2C200&serie1=Optimized+restricted&values_serie1=190989%2C381492%2C571467%2C758550&serie3=Optimized+unrestricted&values_serie3=190935%2C381560%2C572348%2C760212&serie4=Naive+restricted&values_serie4=196908%2C421515%2C646856%2C870234&serie5=Naive+unrestricted&values_serie5=225522%2C502832%2C789038%2C1073485
10 - 10 - 60 (prod3). 30 (consumer)
back and forth
experiment 1

unop unres : 225522,502832,789038,1073485
 50
Total runtime: 134207.
Benchmarking times in each iteration: [44269.0, 43761.0, 44929.0]

100
Total runtime: 293314.
Benchmarking times in each iteration: [96868.0, 95371.0, 98219.0]

150
Total runtime: 473700.
Benchmarking times in each iteration: [152070.0, 156428.0, 158734.0]

200
Total runtime: 656427.
Benchmarking times in each iteration: [207298.0, 222995.0, 218721.0]

--

op unres: 190935,381560,572348,760212
50
Total runtime: 114928.
Benchmarking times in each iteration: [38376.0, 38362.0, 38181.0]

100
Total runtime: 229664.
Benchmarking times in each iteration: [76675.0, 76488.0, 76486.0]

150
Total runtime: 343379.
Benchmarking times in each iteration: [114331.0, 114451.0, 114585.0]

200
Total runtime: 456777.
Benchmarking times in each iteration: [152410.0, 152081.0, 152278.0].
 
--

un res: 196908,421515,646856,870234
 50
Total runtime: 118828.
Benchmarking times in each iteration: [39741.0, 39557.0, 39519.0]

 100
Total runtime: 253822.
Benchmarking times in each iteration: [84789.0, 84698.0, 84327.0]
 
 150
Total runtime: 387352.
Benchmarking times in each iteration: [129114.0, 129056.0, 129171.0]
 
 200
Total runtime: 522020.
Benchmarking times in each iteration: [174261.0, 173897.0, 173853.0]
 
--

op res: 190989,381492,571467,758550
 50
Total runtime: 115190.
Benchmarking times in each iteration: [38436.0, 38338.0, 38408.0]

 100
Total runtime: 228019.
Benchmarking times in each iteration: [76195.0, 75840.0, 75971.0]
 
 150
Total runtime: 341279.
Benchmarking times in each iteration: [113732.0, 113501.0, 114034.0]
 
 200
Total runtime: 454279.
Benchmarking times in each iteration: [151409.0, 151313.0, 151545.0]
--
*/

/*
experiment 2:
one way:
10 10 60 prod - 20 con

op unres:
50:
Total runtime: 153287.
Benchmarking times in each iteration: [51130.0, 50988.0, 51159.0]

100:
Total runtime: 304234.
Benchmarking times in each iteration: [101118.0, 101503.0, 101605.0]

150:
Total runtime: 455057.
Benchmarking times in each iteration: [151595.0, 151284.0, 152167.0]

200:
Total runtime: 608900.
Benchmarking times in each iteration: [203364.0, 202776.0, 202748.0]

unop unres:
50:
Total runtime: 194109.
Benchmarking times in each iteration: [52573.0, 53342.0, 54460.0]

100:
Total runtime: 425060.
Benchmarking times in each iteration: [115425.0, 118492.0, 117434.0]

150:
Total runtime: 653816.
Benchmarking times in each iteration: [179383.0, 181547.0, 182344.0]

200:
Total runtime: 882746.
Benchmarking times in each iteration: [236258.0, 246235.0, 246278.0]

*/

/*
10 10 60 prod - 20 con
back and forth
experiment 1 (new)
optimized restricted:
50:
Total runtime: 115173.
Benchmarking times in each iteration: [38484.0, 38322.0, 38354.0]
100:
Total runtime: 228967.
Benchmarking times in each iteration: [76403.0, 76303.0, 76252.0]
150:
Total runtime: 341315.
Benchmarking times in each iteration: [113959.0, 113677.0, 113671.0]
200:
Total runtime: 455241.
Benchmarking times in each iteration: [151638.0, 151815.0, 151778.0]

unoptimized restricted:
50:
Total runtime: 116101.
Benchmarking times in each iteration: [38990.0, 38538.0, 38563.0]
100:
Total runtime: 248372.
Benchmarking times in each iteration: [82794.0, 82667.0, 82901.0]
150:
Total runtime: 381011.
Benchmarking times in each iteration: [126944.0, 127152.0, 126906.0]
200:
Total runtime: 516880.
Benchmarking times in each iteration: [172452.0, 172342.0, 172078.0]

optimized unrestricted:
50:
Total runtime: 115075.
Benchmarking times in each iteration: [38572.0, 38247.0, 38245.0]
100: 
Total runtime: 226984.
Benchmarking times in each iteration: [75678.0, 75643.0, 75652.0]
150:
Total runtime: 343837.
Benchmarking times in each iteration: [114583.0, 114310.0, 114932.0]
200:
Total runtime: 458197.
Benchmarking times in each iteration: [152746.0, 152679.0, 152762.0]

unoptimized unrestricted
50:
Total runtime: 129897.
Benchmarking times in each iteration: [43146.0, 43362.0, 42786.0]
100:
Total runtime: 293942.
Benchmarking times in each iteration: [96825.0, 97986.0, 97068.0]
150:
Total runtime: 460985.
Benchmarking times in each iteration: [152145.0, 152748.0, 152363.0]
200:
Total runtime: 631537.
Benchmarking times in each iteration: [204796.0, 210353.0, 210610.0]

 */



/*
testing: same amount to every producer, remove this
un - un
Total runtime: 79915.
Benchmarking times in each iteration: [26255.0, 26112.0, 26375.0]. tot amount: 1800

op - un
Total runtime: 79170.
Benchmarking times in each iteration: [26613.0, 26445.0, 26097.0]. tot amount: 1800


op - res
Total runtime: 80004.
Benchmarking times in each iteration: [26473.0, 26452.0, 26984.0]. tot amount: 1800

un - res
Total runtime: 80077.
Benchmarking times in each iteration: [27063.0, 26361.0, 26642.0]. tot amount: 1800
*/