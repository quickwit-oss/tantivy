



#[derive(Default)]
pub struct Token {
    pub offset_from: usize,
    pub offset_to: usize,
    pub position: usize,
    pub term: String,
}

pub trait Analyzer<'a>: Sized {

    type TokenStreamImpl: TokenStream;

    fn analyze(&mut self, text: &'a str) -> Self::TokenStreamImpl;

    fn filter<NewFilter>(self, new_filter: NewFilter) -> ChainAnalyzer<NewFilter, Self>
        where NewFilter: TokenFilterFactory<<Self as Analyzer<'a>>::TokenStreamImpl> {
        ChainAnalyzer {
            head: new_filter,
            tail: self
        }
    }
}

pub trait TokenStream {

    fn advance(&mut self) -> bool;
    
    fn token(&self) -> &Token;
    
    fn token_mut(&mut self) -> &mut Token;

    fn next(&mut self) -> Option<&Token> {
        if self.advance() {
            Some(self.token())
        }
        else {
            None
        }
    }
}


pub struct ChainAnalyzer<HeadTokenFilterFactory, TailAnalyzer> {
    head: HeadTokenFilterFactory,
    tail: TailAnalyzer
}


impl<'a, HeadTokenFilterFactory, TailAnalyzer> Analyzer<'a> for ChainAnalyzer<HeadTokenFilterFactory, TailAnalyzer>
    where HeadTokenFilterFactory: TokenFilterFactory<TailAnalyzer::TokenStreamImpl>,
          TailAnalyzer: Analyzer<'a> {
    
    type TokenStreamImpl = HeadTokenFilterFactory::ResultTokenStream;
    
    fn analyze(&mut self, text: &'a str) -> Self::TokenStreamImpl {
        let tail_token_stream = self.tail.analyze(text);
        self.head.transform(tail_token_stream)
    }
}


pub trait TokenFilterFactory<TailTokenStream: TokenStream> {

    type ResultTokenStream: TokenStream;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream;
}

