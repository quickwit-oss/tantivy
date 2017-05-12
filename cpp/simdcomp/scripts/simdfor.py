#!/usr/bin/env python3


from math import ceil

print("""
/**
* Blablabla
*
*/

""");

def mask(bit):
  return str((1 << bit) - 1)

for length in [32]:
  print("""
static __m128i  iunpackFOR0(__m128i initOffset, const __m128i *   _in , uint32_t *    _out) {
    __m128i       *out = (__m128i*)(_out);
    int i;
    (void) _in;
    for (i = 0; i < 8; ++i) {
        _mm_store_si128(out++, initOffset);
    	_mm_store_si128(out++, initOffset);
        _mm_store_si128(out++, initOffset);
        _mm_store_si128(out++, initOffset);
    }

    return initOffset;
}

  """)
  print("""

static void ipackFOR0(__m128i initOffset , const uint32_t *   _in , __m128i *  out  ) {
    (void) initOffset;
    (void) _in;
    (void) out;
}
""") 
  for bit in range(1,33):
    offsetVar = " initOffset";
    print("""  
static void ipackFOR"""+str(bit)+"""(__m128i """+offsetVar+""", const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;

      """);
    
    if (bit != 32):
      print("    __m128i CurrIn = _mm_load_si128(in);");
      print("    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);");
    else:
      print("    __m128i InReg = _mm_load_si128(in);");
      print("    (void) initOffset;");


    inwordpointer = 0
    valuecounter = 0
    for k in range(ceil((length * bit) / 32)):
      if(valuecounter == length): break
      for x in range(inwordpointer,32,bit):
        if(x!=0) :
          print("    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, " + str(x) + "));");
        else:
          print("    OutReg = InReg; ");
        if((x+bit>=32) ):
          while(inwordpointer<32):
            inwordpointer += bit
          print("    _mm_store_si128(out, OutReg);");
          print("");

          if(valuecounter + 1 < length):
            print("    ++out;")
          inwordpointer -= 32;
          if(inwordpointer>0):
            print("    OutReg = _mm_srli_epi32(InReg, " + str(bit) + " - " + str(inwordpointer) + ");");
        if(valuecounter + 1 < length):
          print("    ++in;") 

          if (bit != 32):
            print("    CurrIn = _mm_load_si128(in);");
            print("    InReg = _mm_sub_epi32(CurrIn, initOffset);");
          else:
            print("    InReg = _mm_load_si128(in);");
          print("");
        valuecounter = valuecounter + 1
        if(valuecounter == length): break
    assert(valuecounter == length)
    print("\n}\n\n""")

  for bit in range(1,32):
    offsetVar = " initOffset";
    print("""\n
static __m128i iunpackFOR"""+str(bit)+"""(__m128i """+offsetVar+""", const  __m128i*   in, uint32_t *   _out) {
      """);
    print("""    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;    
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<"""+str(bit)+""")-1);

    """);

    MainText = "";

    MainText += "\n";
    inwordpointer = 0
    valuecounter = 0
    for k in range(ceil((length * bit) / 32)):
      for x in range(inwordpointer,32,bit):
        if(valuecounter == length): break
        if (x > 0):
          MainText += "    tmp = _mm_srli_epi32(InReg," + str(x) +");\n"; 
        else:
          MainText += "    tmp = InReg;\n"; 
        if(x+bit<32):
          MainText += "    OutReg = _mm_and_si128(tmp, mask);\n";
        else:
          MainText += "    OutReg = tmp;\n";        
        if((x+bit>=32) ):      
          while(inwordpointer<32):
            inwordpointer += bit
          if(valuecounter + 1 < length):
             MainText += "    ++in;"
             MainText += "    InReg = _mm_load_si128(in);\n";
          inwordpointer -= 32;
          if(inwordpointer>0):
            MainText += "    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, " + str(bit) + "-" + str(inwordpointer) + "), mask));\n\n";
        if (bit != 32):
          MainText += "    OutReg = _mm_add_epi32(OutReg, initOffset);\n"; 
        MainText += "    _mm_store_si128(out++, OutReg);\n\n"; 
        MainText += "";
        valuecounter = valuecounter + 1
        if(valuecounter == length): break
    assert(valuecounter == length)
    print(MainText)
    print("    return initOffset;");
    print("\n}\n\n")
  print("""
static __m128i iunpackFOR32(__m128i initvalue , const  __m128i*   in, uint32_t *    _out) {
	__m128i * mout = (__m128i *)_out;
	__m128i invec;
	size_t k;
	for(k = 0; k < 128/4; ++k) {
		invec =  _mm_load_si128(in++);
	    _mm_store_si128(mout++, invec);
	}
	return invec;
}
  """)
