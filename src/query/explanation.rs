use std::fmt;
use std::iter;

#[derive(RustcDecodable)]
pub struct Explanation {
    val: f32,
    description: String,
    formula: String,
    children: Vec<(String, Explanation)>,
}


impl Explanation {

    pub fn with_val(val: f32) -> Explanation {
        Explanation {
            val: val,
            description: String::new(),
            formula: String::new(),
            children: Vec::new(),
        }
    }

    pub fn val(&self,) -> f32 {
        self.val
    }
    

    pub fn description(&mut self, description: &str) {
        self.description.clear();
        self.description.push_str(description);
    }

    pub fn set_formula(&mut self, formula: &str) {
        self.formula.clear();
        self.formula.push_str(formula);
    }

    pub fn add_child(&mut self, name: &str, val: f32) -> &mut Explanation {
        let explanation = Explanation::with_val(val);
        let name = String::from(name);
        self.children.push((name, explanation));
        let &mut (_, ref mut child_experience) = self.children.last_mut().unwrap();
        child_experience
    }

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