use std::sync::Arc;
use stemmer;


pub struct StemmerTokenStream<TailTokenStream> 
    where TailTokenStream: TokenStream {
    tail: TailTokenStream,
    stemmer: Arc<stemmer::Stemmer>,
}

impl<TailTokenStream> TokenStream for StemmerTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream {

    fn token(&self) -> &Token {
        self.tail.token()
    }
    
    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }

    fn advance(&mut self) -> bool {
        if self.tail.advance() {
            // self.tail.token_mut().term.make_ascii_lowercase();
            let new_str = self.stemmer.stem_str(&self.token().term);
            true
        }
        else {
            false
        }
    }

}

impl<TailTokenStream> StemmerTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream {
    
    fn wrap(stemmer: Arc<stemmer::Stemmer>, tail: TailTokenStream) -> StemmerTokenStream<TailTokenStream> {
        StemmerTokenStream {
            tail,
            stemmer,
        }
    } 
}