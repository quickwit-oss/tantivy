use std::fmt;
use std::iter;

/// Tree representing the expression of the score of document.
/// The explanation is organized as follows.
/// 
/// - **val** is the value of the expression
/// - **formula** is a string representing a math formula.
///   The formula may contain named sub expressions. These subexpression 
///   should be marked `<expr_name>`.
/// - **description** is a short markdown text that may help the user to
///   understand the score.   
/// - The explanation of the sub expression is recursively explained in the
///   children structure.
///
#[derive(RustcEncodable)]
pub struct Explanation {
    val: f32,
    description: String,
    formula: String,
    children: Vec<(String, Explanation)>,
}


impl Explanation {
    
    /// Create an empty explanation for a given value. 
    pub fn with_val(val: f32) -> Explanation {
        Explanation {
            val: val,
            description: String::new(),
            formula: String::new(),
            children: Vec::new(),
        }
    }
    
    /// Accessor for the value
    pub fn val(&self,) -> f32 {
        self.val
    }
    
    /// Accessor for the description
    pub fn description(&mut self, description: &str) {
        self.description.clear();
        self.description.push_str(description);
    }
    
    /// Sets the formula
    pub fn set_formula(&mut self, formula: &str) {
        self.formula.clear();
        self.formula.push_str(formula);
    }
    
    /// Add an explanation for a component of our formula
    /// - name is the name of the component. It should 
    ///   appear as `<the_name>` in the formula.
    /// - val is the value of the component.
    pub fn add_child(&mut self, name: &str, val: f32) -> &mut Explanation {
        let explanation = Explanation::with_val(val);
        let name = String::from(name);
        self.children.push((name, explanation));
        let &mut (_, ref mut child_experience) = self.children.last_mut().unwrap();
        child_experience
    }
    
    
    /// Creates a `String` from the explanation.
    /// The subcomponent tree is represented by increasing 
    /// the indentation with the tree-depth.
    pub fn format_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        let padding: String = iter::repeat(' ').take(indent).collect();
        try!(write!(f, "{}{}: {}\n", padding, self.val, self.description));
        if !self.formula.is_empty() {
            try!(write!(f, "{}: {}\n", padding, self.formula));       
        }
        for &(ref child_name, ref child) in &self.children {
            try!(write!(f, "- {}:\n", child_name));
            try!(child.format_with_indent(f, indent + 2));
        }
        Ok(())
    }
}

impl fmt::Debug for Explanation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.format_with_indent(f, 0)
    }
}