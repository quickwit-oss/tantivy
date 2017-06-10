use std::borrow::{Borrow, BorrowMut};

/// Token 
pub struct Token {
    /// Offset (byte index) of the first character of the token.
    /// Offsets shall not be modified by token filters.
    pub offset_from: usize,
    /// Offset (byte index) of the last character of the token + 1.
    /// The text that generated the token should be obtained by 
    /// &text[token.offset_from..token.offset_to]
    pub offset_to: usize,
    /// Position, expressed in number of tokens.
    pub position: usize,
    /// Actual text content of the token.
    pub term: String,
}

impl Default for Token {
    fn default() -> Token {
        Token {
            offset_from: 0,
            offset_to: 0,
            position: usize::max_value(),
            term: String::new(),
        }
    }
}

pub trait Analyzer<'a>: Sized {
    type TokenStreamImpl: TokenStream;

    fn token_stream(&mut self, text: &'a str) -> Self::TokenStreamImpl;

    fn filter<NewFilter>(self, new_filter: NewFilter) -> ChainAnalyzer<NewFilter, Self>
        where NewFilter: TokenFilterFactory<<Self as Analyzer<'a>>::TokenStreamImpl>
    {
        ChainAnalyzer {
            head: new_filter,
            tail: self,
        }
    }
}

pub trait BoxedAnalyzer {
    fn token_stream<'a>(&mut self, text: &'a str) -> Box<TokenStream + 'a>;    
}

struct BoxableAnalyzer<A>(A) where A: for <'a> Analyzer<'a>;

impl<A> BoxedAnalyzer for BoxableAnalyzer<A> where A: 'static + for <'a> Analyzer<'a> {
    fn token_stream<'b>(&mut self, text: &'b str) -> Box<TokenStream + 'b> {
        box self.0.token_stream(text)
    }
}

pub fn box_analyzer<A>(a: A) -> Box<BoxedAnalyzer>
    where A: 'static + for <'a> Analyzer<'a> {
    box BoxableAnalyzer(a)
}


impl<'b> TokenStream for Box<TokenStream + 'b> {
    fn advance(&mut self) -> bool {
        let token_stream: &mut TokenStream = self.borrow_mut();
        token_stream.advance()
    }

    fn token(&self) -> &Token {
        let token_stream: &TokenStream = self.borrow();
        token_stream.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        let token_stream: &mut TokenStream = self.borrow_mut();
        token_stream.token_mut()
    }
}


pub trait TokenStream {
    fn advance(&mut self) -> bool;

    fn token(&self) -> &Token;

    fn token_mut(&mut self) -> &mut Token;

    fn next(&mut self) -> Option<&Token> {
        if self.advance() {
            Some(self.token())
        } else {
            None
        }
    }

    fn process(&mut self, sink: &mut FnMut(&Token)) -> u32 {
        let mut num_tokens_pushed = 0u32;
        while self.advance() {
            sink(self.token());
            num_tokens_pushed += 1u32;
        }
        num_tokens_pushed
    }
}


pub struct ChainAnalyzer<HeadTokenFilterFactory, TailAnalyzer> {
    head: HeadTokenFilterFactory,
    tail: TailAnalyzer,
}


impl<'a, HeadTokenFilterFactory, TailAnalyzer> Analyzer<'a>
    for ChainAnalyzer<HeadTokenFilterFactory, TailAnalyzer>
    where HeadTokenFilterFactory: TokenFilterFactory<TailAnalyzer::TokenStreamImpl>,
          TailAnalyzer: Analyzer<'a>
{
    type TokenStreamImpl = HeadTokenFilterFactory::ResultTokenStream;

    fn token_stream(&mut self, text: &'a str) -> Self::TokenStreamImpl {
        let tail_token_stream = self.tail.token_stream(text);
        self.head.transform(tail_token_stream)
    }
}


pub trait TokenFilterFactory<TailTokenStream: TokenStream> {
    type ResultTokenStream: TokenStream;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream;
}
